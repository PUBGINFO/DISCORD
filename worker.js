import { verifyKey } from 'discord-interactions';

// Cloudflare Worker 메인 핸들러
export default {
  async fetch(request, env, ctx) {
    if (request.method === 'GET') {
      return new Response('Discord Server Cloner Bot is running!', { status: 200 });
    }

    const signature = request.headers.get('X-Signature-Ed25519');
    const timestamp = request.headers.get('X-Signature-Timestamp');
    const bodyText = await request.text();

    // 디스코드 요청 검증 (보안)
    const isValidRequest = verifyKey(
      bodyText,
      signature,
      timestamp,
      env.DISCORD_PUBLIC_KEY
    );

    if (!isValidRequest) {
      return new Response('Invalid request signature', { status: 401 });
    }

    const interaction = JSON.parse(bodyText);

    // 1. Ping (디스코드 봇 최초 등록 시 필수)
    if (interaction.type === 1) {
      return new Response(JSON.stringify({ type: 1 }), {
        headers: { 'Content-Type': 'application/json' },
      });
    }

    // 2. 슬래시 커맨드 처리
    if (interaction.type === 2) {
      const { name } = interaction.data;
      const guildId = interaction.guild_id;
      const token = env.DISCORD_BOT_TOKEN;

      if (name === 'backup') {
        ctx.waitUntil(handleBackup(guildId, token, env));
        return jsonResponse({
          type: 4,
          data: { content: '📦 서버 구조 백업을 시작합니다! 데이터가 KV에 저장됩니다.' },
        });
      }

      if (name === 'clone') {
        ctx.waitUntil(handleClone(guildId, token, env));
        return jsonResponse({
          type: 4,
          data: { content: '🚀 저장된 백업 데이터를 바탕으로 서버 구조 복제를 시작합니다!' },
        });
      }
    }

    return new Response('Bad request', { status: 400 });
  },
};

function jsonResponse(obj) {
  return new Response(JSON.stringify(obj), {
    headers: { 'Content-Type': 'application/json' },
  });
}

// 서버 구조 백업 로직 (역할 및 채널 정보를 KV에 저장)
async function handleBackup(guildId, token, env) {
  try {
    // 1. 역할(Roles) 가져오기
    const rolesRes = await fetch(`https://discord.com/api/v10/guilds/${guildId}/roles`, {
      headers: { Authorization: `Bot ${token}` },
    });
    const roles = await rolesRes.json();

    // 2. 채널(Channels) 가져오기
    const channelsRes = await fetch(`https://discord.com/api/v10/guilds/${guildId}/channels`, {
      headers: { Authorization: `Bot ${token}` },
    });
    const channels = await channelsRes.json();

    const backupData = {
      roles: roles.filter(r => r.name !== '@everyone'), // everyone 역할 제외
      channels: channels.map(c => ({
        name: c.name,
        type: c.type, // 0: 텍스트, 2: 음성, 4: 카테고리
        position: c.position,
        parent_id: c.parent_id,
        topic: c.topic,
        bitrate: c.bitrate,
        user_limit: c.user_limit,
      })),
    };

    // Cloudflare KV에 저장
    await env.SERVER_BACKUPS.put(`backup_${guildId}`, JSON.stringify(backupData));
    console.log(`서버 ${guildId} 백업 완료`);
  } catch (err) {
    console.error('백업 실패:', err);
  }
}

// 서버 구조 복제/복원 로직
async function handleClone(guildId, token, env) {
  try {
    const dataStr = await env.SERVER_BACKUPS.get(`backup_${guildId}`);
    if (!dataStr) return;
    const backupData = JSON.parse(dataStr);

    // 1. 역할 생성
    const roleMap = {};
    for (const role of backupData.roles.reverse()) {
      const res = await fetch(`https://discord.com/api/v10/guilds/${guildId}/roles`, {
        method: 'POST',
        headers: {
          Authorization: `Bot ${token}`,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          name: role.name,
          permissions: role.permissions,
          color: role.color,
          hoist: role.hoist,
          mentionable: role.mentionable,
        }),
      });
      if (res.ok) {
        const newRole = await res.json();
        roleMap[role.id] = newRole.id;
      }
      await new Promise(r => setTimeout(r, 500));
    }

    // 2. 카테고리 먼저 생성
    const categories = backupData.channels.filter(c => c.type === 4);
    const categoryMap = {};

    for (const cat of categories) {
      const res = await fetch(`https://discord.com/api/v10/guilds/${guildId}/channels`, {
        method: 'POST',
        headers: {
          Authorization: `Bot ${token}`,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          name: cat.name,
          type: 4,
          position: cat.position,
        }),
      });
      if (res.ok) {
        const newCat = await res.json();
        categoryMap[cat.id] = newCat.id;
      }
      await new Promise(r => setTimeout(r, 500));
    }

    // 3. 일반 채널(텍스트/음성) 생성
    const otherChannels = backupData.channels.filter(c => c.type !== 4);
    for (const ch of otherChannels) {
      let parent_id = null;
      if (ch.parent_id && categoryMap[ch.parent_id]) {
        parent_id = categoryMap[ch.parent_id];
      }

      await fetch(`https://discord.com/api/v10/guilds/${guildId}/channels`, {
        method: 'POST',
        headers: {
          Authorization: `Bot ${token}`,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          name: ch.name,
          type: ch.type,
          position: ch.position,
          parent_id: parent_id,
          topic: ch.topic,
          bitrate: ch.bitrate,
          user_limit: ch.user_limit,
        }),
      });
      await new Promise(r => setTimeout(r, 500));
    }

    console.log(`서버 ${guildId} 복제 완료`);
  } catch (err) {
    console.error('복제 실패:', err);
  }
}
