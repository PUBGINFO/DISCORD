/**
 * Discord Server Cloner
 * Cloudflare Workers + Queues + KV
 *
 * Commands:
 * /serverid
 * /channelid
 * /backup [guild_id]
 * /clone source_guild_id target_guild_id
 *
 * Bindings:
 * SERVER_BACKUPS : KV
 * CLONE_QUEUE     : Queue
 *
 * Vars:
 * DISCORD_BOT_TOKEN
 * DISCORD_APPLICATION_ID
 * DISCORD_PUBLIC_KEY
 */

const DISCORD_API = 'https://discord.com/api/v10';

const MAX_ROLES_PER_RUN = 5;
const MAX_CATEGORIES_PER_RUN = 5;
const MAX_CHANNELS_PER_RUN = 5;

const JOB_LOCK_MS = 55000;
const PROGRESS_MIN_INTERVAL_MS = 1200;
const MAX_RETRIES = 3;

const TEMPORARY_ERRORS = new Set([
  408, 425, 429, 500, 502, 503, 504
]);

/* ============================================================
 * Worker
 * ========================================================== */

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    if (
      request.method === 'GET' &&
      url.pathname === '/'
    ) {
      return new Response(
        'Discord Server Cloner Worker is running securely.',
        {
          status: 200,
          headers: {
            'Content-Type': 'text/plain; charset=utf-8',
            'Cache-Control': 'no-store'
          }
        }
      );
    }

    if (
      request.method === 'GET' &&
      url.pathname === '/register'
    ) {
      try {
        const result =
          await registerSlashCommands(
            env.DISCORD_BOT_TOKEN,
            env.DISCORD_APPLICATION_ID
          );

        return new Response(
          result.text,
          {
            status: result.status,
            headers: {
              'Content-Type':
                'text/plain; charset=utf-8'
            }
          }
        );
      } catch (err) {
        console.error(
          '[REGISTER ERROR]',
          err?.stack || err
        );

        return new Response(
          `등록 실패: ${errorMessage(err)}`,
          { status: 500 }
        );
      }
    }

    if (request.method !== 'POST') {
      return new Response(
        'Method Not Allowed',
        { status: 405 }
      );
    }

    const signature =
      request.headers.get(
        'X-Signature-Ed25519'
      );

    const timestamp =
      request.headers.get(
        'X-Signature-Timestamp'
      );

    const body =
      await request.text();

    const valid =
      await verifyDiscordRequest(
        signature,
        timestamp,
        body,
        env.DISCORD_PUBLIC_KEY
      );

    if (!valid) {
      console.error(
        '[SIGNATURE ERROR] Invalid Discord signature'
      );

      return new Response(
        'Invalid request signature',
        { status: 401 }
      );
    }

    let interaction;

    try {
      interaction = JSON.parse(body);
    } catch {
      return new Response(
        'Invalid JSON',
        { status: 400 }
      );
    }

    if (interaction.type === 1) {
      return json({ type: 1 });
    }

    if (interaction.type === 2) {
      return handleSlashCommand(
        interaction,
        env,
        ctx
      );
    }

    return new Response(
      'Bad Request',
      { status: 400 }
    );
  },

  async queue(batch, env) {
    console.log(
      `[QUEUE] batch=${batch.messages.length}`
    );

    for (const message of batch.messages) {
      const jobId =
        message.body?.jobId;

      if (!jobId) {
        message.ack();
        continue;
      }

      console.log(
        `[QUEUE] job=${jobId}`
      );

      try {
        const result =
          await processCloneJob(
            jobId,
            env
          );

        if (result === 'retry') {
          console.log(
            `[QUEUE RETRY] job=${jobId}`
          );

          message.retry();
        } else {
          message.ack();
        }
      } catch (err) {
        console.error(
          `[QUEUE EXCEPTION] job=${jobId}`,
          err?.stack || err
        );

        message.retry();
      }
    }
  }
};


/* ============================================================
 * Slash Commands
 * ========================================================== */

async function handleSlashCommand(
  interaction,
  env,
  ctx
) {
  const name =
    interaction.data?.name;

  const options =
    interaction.data?.options || [];

  const currentGuildId =
    interaction.guild_id;

  if (name === 'serverid') {
    return json({
      type: 4,
      data: {
        content:
          `📌 **현재 서버 ID:** \`${currentGuildId || 'DM'}\``
      }
    });
  }

  if (name === 'channelid') {
    return json({
      type: 4,
      data: {
        content:
          `📌 **현재 채널 ID:** \`${interaction.channel?.id || '알 수 없음'}\``
      }
    });
  }

  if (name === 'backup') {
    const option =
      options.find(
        x => x.name === 'guild_id'
      );

    const guildId =
      String(
        option?.value ||
        currentGuildId ||
        ''
      ).trim();

    if (!guildId) {
      return json({
        type: 4,
        data: {
          content:
            '❌ 백업할 서버 ID를 확인할 수 없습니다.'
        }
      });
    }

    ctx.waitUntil(
      handleBackup(
        guildId,
        env,
        interaction.application_id ||
          env.DISCORD_APPLICATION_ID,
        interaction.token
      )
    );

    return json({
      type: 5
    });
  }

  if (name === 'clone') {
    const source =
      String(
        options.find(
          x =>
            x.name ===
            'source_guild_id'
        )?.value || ''
      ).trim();

    const target =
      String(
        options.find(
          x =>
            x.name ===
            'target_guild_id'
        )?.value || ''
      ).trim();

    if (!source || !target) {
      return json({
        type: 4,
        data: {
          content:
            '❌ 사용법: `/clone source_guild_id target_guild_id`'
        }
      });
    }

    if (source === target) {
      return json({
        type: 4,
        data: {
          content:
            '❌ 원본 서버와 대상 서버는 같을 수 없습니다.'
        }
      });
    }

    const jobId =
      `job_${Date.now()}_${Math.random()
        .toString(36)
        .slice(2, 8)}`;

    const job = {
      jobId,

      sourceGuildId: source,
      targetGuildId: target,

      applicationId:
        interaction.application_id ||
        env.DISCORD_APPLICATION_ID,

      interactionToken:
        interaction.token,

      phase: 'INIT',
      status: 'running',

      roleIndex: 0,
      categoryIndex: 0,
      channelIndex: 0,

      roleSuccess: 0,
      roleFailed: 0,

      categorySuccess: 0,
      categoryFailed: 0,

      channelSuccess: 0,
      channelFailed: 0,

      roleMap: {},
      categoryMap: {},
      channelMap: {},

      rolePositionMap: {},

      processedRoles: {},
      processedCategories: {},
      processedChannels: {},

      roleRetries: {},
      categoryRetries: {},
      channelRetries: {},

      targetEveryoneRoleId:
        target,

      botHighestRolePosition: 0,

      lastProgress: '',
      lastProgressAt: 0,

      lockedUntil: 0,

      createdAt: Date.now(),
      startedAt: Date.now(),

      lastError: null
    };

    await saveJob(
      env,
      job
    );

    if (!env.CLONE_QUEUE) {
      console.error(
        '[QUEUE ERROR] CLONE_QUEUE binding missing'
      );

      return json({
        type: 4,
        data: {
          content:
            '❌ CLONE_QUEUE가 설정되어 있지 않습니다.'
        }
      });
    }

    await env.CLONE_QUEUE.send({
      jobId
    });

    console.log(
      `[CLONE CREATE] job=${jobId} source=${source} target=${target}`
    );

    return json({
      type: 5
    });
  }

  return json({
    type: 4,
    data: {
      content:
        '❌ 알 수 없는 명령어입니다.'
    }
  });
}


/* ============================================================
 * Backup
 * ========================================================== */

async function handleBackup(
  guildId,
  env,
  applicationId,
  interactionToken
) {
  const startedAt =
    Date.now();

  try {
    await updateProgress(
      applicationId,
      interactionToken,
      `📦 **서버 백업을 시작합니다...**\n\n` +
      `🎯 서버: \`${guildId}\`\n` +
      `⏳ 서버 정보를 가져오는 중...`
    );

    const rolesRes =
      await discordApi(
        `/guilds/${guildId}/roles`,
        env.DISCORD_BOT_TOKEN,
        {},
        '백업 역할 조회'
      );

    if (!rolesRes.ok) {
      throw permanentError(
        `역할 조회 실패 (${rolesRes.status})`
      );
    }

    const channelsRes =
      await discordApi(
        `/guilds/${guildId}/channels`,
        env.DISCORD_BOT_TOKEN,
        {},
        '백업 채널 조회'
      );

    if (!channelsRes.ok) {
      throw permanentError(
        `채널 조회 실패 (${channelsRes.status})`
      );
    }

    const roles =
      Array.isArray(
        rolesRes.data
      )
        ? rolesRes.data
        : [];

    const channels =
      Array.isArray(
        channelsRes.data
      )
        ? channelsRes.data
        : [];

    const rolePositionMap = {};

    for (const role of roles) {
      rolePositionMap[role.id] =
        Number(
          role.position || 0
        );
    }

    const backupData = {
      guildId,

      roles: roles.map(role => ({
        id: role.id,
        name: role.name,
        permissions:
          String(
            role.permissions || '0'
          ),
        color:
          Number(
            role.color || 0
          ),
        hoist:
          !!role.hoist,
        mentionable:
          !!role.mentionable,
        managed:
          !!role.managed,
        position:
          Number(
            role.position || 0
          )
      })),

      channels: channels.map(channel => ({
        id: channel.id,

        name:
          channel.name,

        type:
          Number(channel.type),

        position:
          Number(
            channel.position || 0
          ),

        parent_id:
          channel.parent_id ||
          null,

        topic:
          channel.topic !==
          undefined
            ? channel.topic
            : null,

        bitrate:
          channel.bitrate !==
          undefined
            ? channel.bitrate
            : null,

        user_limit:
          channel.user_limit !==
          undefined
            ? channel.user_limit
            : null,

        rate_limit_per_user:
          channel.rate_limit_per_user !==
          undefined
            ? channel.rate_limit_per_user
            : null,

        nsfw:
          channel.nsfw !==
          undefined
            ? !!channel.nsfw
            : null,

        permission_overwrites:
          Array.isArray(
            channel.permission_overwrites
          )
            ? channel.permission_overwrites
            : []
      }))
    };

    await env.SERVER_BACKUPS.put(
      `backup_${guildId}`,
      JSON.stringify(
        backupData
      )
    );

    const elapsed =
      (
        (Date.now() -
          startedAt) /
        1000
      ).toFixed(1);

    await updateProgress(
      applicationId,
      interactionToken,
      `✅ 서버(\`${guildId}\`) 백업이 완료되었습니다!\n\n` +
      `👤 역할: ${roles.length}\n` +
      `💬 채널: ${channels.length}\n` +
      `⏱️ 소요시간: ${elapsed}초`,
      true
    );

    console.log(
      `[BACKUP COMPLETE] guild=${guildId} roles=${roles.length} channels=${channels.length}`
    );
  } catch (err) {
    console.error(
      `[BACKUP ERROR] guild=${guildId}`,
      err?.stack || err
    );

    await updateProgress(
      applicationId,
      interactionToken,
      `❌ 서버 백업에 실패했습니다.\n\n` +
      `사유: \`${escapeDiscordText(errorMessage(err))}\``,
      true
    );
  }
}


/* ============================================================
 * Clone Job
 * ========================================================== */

async function processCloneJob(
  jobId,
  env
) {
  const raw =
    await env.SERVER_BACKUPS.get(
      jobId
    );

  if (!raw) {
    console.log(
      `[JOB SKIP] job=${jobId} data-not-found`
    );

    return 'ack';
  }

  let job;

  try {
    job =
      JSON.parse(raw);
  } catch {
    console.error(
      `[JOB ERROR] job=${jobId} invalid-json`
    );

    return 'ack';
  }

  if (
    job.status !==
    'running'
  ) {
    console.log(
      `[JOB SKIP] job=${jobId} status=${job.status}`
    );

    return 'ack';
  }

  const now =
    Date.now();

  if (
    job.lockedUntil &&
    job.lockedUntil > now
  ) {
    console.log(
      `[JOB SKIP] job=${jobId} already-running`
    );

    return 'ack';
  }

  job.lockedUntil =
    now + JOB_LOCK_MS;

  await saveJob(
    env,
    job
  );

  const progress =
    createProgressUpdater(
      env,
      job
    );

  try {
    const backupRaw =
      await env.SERVER_BACKUPS.get(
        `backup_${job.sourceGuildId}`
      );

    if (!backupRaw) {
      throw permanentError(
        '원본 서버의 백업 데이터가 없습니다. 먼저 /backup을 실행해주세요.'
      );
    }

    let backup;

    try {
      backup =
        JSON.parse(
          backupRaw
        );
    } catch {
      throw permanentError(
        '백업 데이터가 손상되었습니다.'
      );
    }

    const roles =
      Array.isArray(
        backup.roles
      )
        ? [...backup.roles]
        : [];

    const allChannels =
      Array.isArray(
        backup.channels
      )
        ? [...backup.channels]
        : [];

    const categories =
      allChannels
        .filter(
          c =>
            Number(c.type) === 4
        )
        .sort(
          (a, b) =>
            Number(a.position || 0) -
            Number(b.position || 0)
        );

    const normalChannels =
      allChannels
        .filter(
          c =>
            Number(c.type) !== 4
        )
        .sort(
          (a, b) =>
            Number(a.position || 0) -
            Number(b.position || 0)
        );

    /*
     * INIT
     */

    if (
      job.phase ===
      'INIT'
    ) {
      const setup =
        await prepareTargetServer(
          job,
          env
        );

      job.targetEveryoneRoleId =
        setup.everyoneRoleId;

      job.botHighestRolePosition =
        setup.botHighestRolePosition;

      job.phase =
        'ROLES';

      await saveJob(
        env,
        job
      );

      await progress(
        buildProgress(
          job,
          roles,
          categories,
          normalChannels
        )
      );
    }

    /*
     * ROLES
     */

    if (
      job.phase ===
      'ROLES'
    ) {
      const result =
        await processRoles(
          job,
          roles,
          env
        );

      await saveJob(
        env,
        job
      );

      if (
        result ===
        'retry'
      ) {
        job.lockedUntil = 0;

        await saveJob(
          env,
          job
        );

        return 'retry';
      }

      if (
        job.roleIndex <
        roles.length
      ) {
        await progress(
          buildProgress(
            job,
            roles,
            categories,
            normalChannels
          )
        );

        await enqueueNext(
          env,
          job
        );

        job.lockedUntil = 0;

        await saveJob(
          env,
          job
        );

        return 'ack';
      }

      job.phase =
        'ROLES_SYNC';

      await saveJob(
        env,
        job
      );
    }

    /*
     * ROLE SYNC
     */

    if (
      job.phase ===
      'ROLES_SYNC'
    ) {
      await synchronizeRoleHierarchy(
        job,
        roles,
        env
      );

      job.phase =
        'CATEGORIES';

      await saveJob(
        env,
        job
      );
    }

    /*
     * CATEGORIES
     */

    if (
      job.phase ===
      'CATEGORIES'
    ) {
      const result =
        await processCategories(
          job,
          categories,
          env
        );

      await saveJob(
        env,
        job
      );

      if (
        result ===
        'retry'
      ) {
        job.lockedUntil = 0;

        await saveJob(
          env,
          job
        );

        return 'retry';
      }

      if (
        job.categoryIndex <
        categories.length
      ) {
        await progress(
          buildProgress(
            job,
            roles,
            categories,
            normalChannels
          )
        );

        await enqueueNext(
          env,
          job
        );

        job.lockedUntil = 0;

        await saveJob(
          env,
          job
        );

        return 'ack';
      }

      job.phase =
        'CHANNELS';

      await saveJob(
        env,
        job
      );
    }

    /*
     * CHANNELS
     */

    if (
      job.phase ===
      'CHANNELS'
    ) {
      const result =
        await processChannels(
          job,
          normalChannels,
          env
        );

      await saveJob(
        env,
        job
      );

      if (
        result ===
        'retry'
      ) {
        job.lockedUntil = 0;

        await saveJob(
          env,
          job
        );

        return 'retry';
      }

      if (
        job.channelIndex <
        normalChannels.length
      ) {
        await progress(
          buildProgress(
            job,
            roles,
            categories,
            normalChannels
          )
        );

        await enqueueNext(
          env,
          job
        );

        job.lockedUntil = 0;

        await saveJob(
          env,
          job
        );

        return 'ack';
      }

      job.phase =
        'CHANNEL_SYNC';

      await saveJob(
        env,
        job
      );
    }

    /*
     * CHANNEL SYNC
     */

    if (
      job.phase ===
      'CHANNEL_SYNC'
    ) {
      await synchronizeChannelPositions(
        job,
        categories,
        normalChannels,
        env
      );

      job.phase =
        'COMPLETED';

      job.status =
        'completed';

      await saveJob(
        env,
        job
      );
    }

    /*
     * COMPLETE
     */

    if (
      job.phase ===
      'COMPLETED'
    ) {
      const elapsed =
        (
          (Date.now() -
            job.startedAt) /
          1000
        ).toFixed(1);

      const totalFailed =
        job.roleFailed +
        job.categoryFailed +
        job.channelFailed;

      let content;

      if (
        totalFailed === 0
      ) {
        content =
          `🎉 **서버 복제가 완료되었습니다!**\n\n` +
          `👤 역할: ${job.roleSuccess}/${roles.length}\n` +
          `🔃 역할 순서: 동기화 완료\n` +
          `📁 카테고리: ${job.categorySuccess}/${categories.length}\n` +
          `💬 채널: ${job.channelSuccess}/${normalChannels.length}\n` +
          `⏱️ 소요 시간: ${elapsed}초`;
      } else {
        content =
          `⚠️ **서버 복제가 완료되었지만 일부 항목을 건너뛰었습니다.**\n\n` +
          `👤 역할: ${job.roleSuccess}/${roles.length} (실패 ${job.roleFailed})\n` +
          `📁 카테고리: ${job.categorySuccess}/${categories.length} (실패 ${job.categoryFailed})\n` +
          `💬 채널: ${job.channelSuccess}/${normalChannels.length} (실패 ${job.channelFailed})\n` +
          `⏱️ 소요 시간: ${elapsed}초`;
      }

      await progress(
        content,
        true
      );

      job.lockedUntil =
        0;

      await saveJob(
        env,
        job
      );

      console.log(
        `[JOB COMPLETE] job=${jobId}`
      );

      return 'ack';
    }

    job.lockedUntil =
      0;

    await saveJob(
      env,
      job
    );

    return 'ack';

  } catch (err) {
    console.error(
      `[JOB ERROR] job=${jobId}`,
      err?.stack || err
    );

    if (
      isPermanentError(err)
    ) {
      job.status =
        'failed';

      job.lastError =
        errorMessage(err);

      job.failedAt =
        Date.now();

      job.lockedUntil =
        0;

      await saveJob(
        env,
        job
      );

      await progress(
        `❌ **서버 복제에 실패했습니다.**\n\n` +
        `사유: \`${escapeDiscordText(job.lastError)}\``,
        true
      );

      return 'ack';
    }

    job.lockedUntil =
      0;

    await saveJob(
      env,
      job
    );

    return 'retry';
  }
}


/* ============================================================
 * Target Server
 * ========================================================== */

async function prepareTargetServer(
  job,
  env
) {
  const token =
    env.DISCORD_BOT_TOKEN;

  const guild =
    await discordApi(
      `/guilds/${job.targetGuildId}`,
      token,
      {},
      '대상 서버 확인'
    );

  if (!guild.ok) {
    throw permanentError(
      `대상 서버에 접근할 수 없습니다. HTTP ${guild.status}`
    );
  }

  const rolesRes =
    await discordApi(
      `/guilds/${job.targetGuildId}/roles`,
      token,
      {},
      '대상 역할 조회'
    );

  if (!rolesRes.ok) {
    throw new Error(
      `대상 역할 조회 실패 (${rolesRes.status})`
    );
  }

  const roles =
    Array.isArray(
      rolesRes.data
    )
      ? rolesRes.data
      : [];

  const everyone =
    roles.find(
      role =>
        role.id ===
        job.targetGuildId
    );

  const everyoneRoleId =
    everyone?.id ||
    job.targetGuildId;

  const me =
    await discordApi(
      `/users/@me`,
      token,
      {},
      '봇 정보'
    );

  let botHighestRolePosition = 0;

  if (
    me.ok &&
    me.data?.id
  ) {
    const member =
      await discordApi(
        `/guilds/${job.targetGuildId}/members/${me.data.id}`,
        token,
        {},
        '봇 멤버 정보'
      );

    if (member.ok) {
      const botRoleIds =
        Array.isArray(
          member.data?.roles
        )
          ? member.data.roles
          : [];

      for (const role of roles) {
        if (
          role.id !==
            everyoneRoleId &&
          botRoleIds.includes(
            role.id
          )
        ) {
          botHighestRolePosition =
            Math.max(
              botHighestRolePosition,
              Number(
                role.position || 0
              )
            );
        }
      }
    }
  }

  console.log(
    `[SERVER READY] guild=${job.targetGuildId} botPosition=${botHighestRolePosition}`
  );

  return {
    everyoneRoleId,
    botHighestRolePosition
  };
}


/* ============================================================
 * Roles
 * ========================================================== */

async function processRoles(
  job,
  roles,
  env
) {
  const token =
    env.DISCORD_BOT_TOKEN;

  roles.sort(
    (a, b) =>
      Number(a.position || 0) -
      Number(b.position || 0)
  );

  const end =
    Math.min(
      job.roleIndex +
        MAX_ROLES_PER_RUN,
      roles.length
    );

  while (
    job.roleIndex < end
  ) {
    const role =
      roles[job.roleIndex];

    job.rolePositionMap[
      role.id
    ] =
      Number(
        role.position || 0
      );

    if (
      role.name ===
      '@everyone'
    ) {
      job.roleMap[
        role.id
      ] =
        job.targetEveryoneRoleId;

      job.processedRoles[
        role.id
      ] = true;

      job.roleIndex++;

      continue;
    }

    if (role.managed) {
      job.processedRoles[
        role.id
      ] = true;

      job.roleIndex++;

      continue;
    }

    if (
      job.processedRoles[
        role.id
      ]
    ) {
      job.roleIndex++;
      continue;
    }

    const body = {
      name:
        String(
          role.name || ''
        ).slice(0, 100),

      permissions:
        String(
          role.permissions || '0'
        ),

      color:
        Number(
          role.color || 0
        ),

      hoist:
        !!role.hoist,

      mentionable:
        !!role.mentionable
    };

    const res =
      await discordApi(
        `/guilds/${job.targetGuildId}/roles`,
        token,
        {
          method: 'POST',
          headers: {
            'Content-Type':
              'application/json'
          },
          body:
            JSON.stringify(body)
        },
        `역할 생성 "${role.name}"`
      );

    if (
      res.ok &&
      res.data?.id
    ) {
      job.roleSuccess++;

      job.roleMap[
        role.id
      ] =
        res.data.id;

      job.processedRoles[
        role.id
      ] = true;

      job.roleIndex++;

      continue;
    }

    if (
      res.status === 429 ||
      TEMPORARY_ERRORS.has(
        res.status
      )
    ) {
      const count =
        Number(
          job.roleRetries[
            role.id
          ] || 0
        );

      if (
        count < MAX_RETRIES
      ) {
        job.roleRetries[
          role.id
        ] =
          count + 1;

        return 'retry';
      }
    }

    job.roleFailed++;

    job.processedRoles[
      role.id
    ] = true;

    job.roleIndex++;
  }

  return 'ok';
}


/* ============================================================
 * Role Hierarchy
 * ========================================================== */

async function synchronizeRoleHierarchy(
  job,
  sourceRoles,
  env
) {
  if (
    !job.roleMap ||
    !Object.keys(
      job.roleMap
    ).length
  ) {
    return;
  }

  const targetRes =
    await discordApi(
      `/guilds/${job.targetGuildId}/roles`,
      env.DISCORD_BOT_TOKEN,
      {},
      '역할 계층 확인'
    );

  if (!targetRes.ok) {
    console.log(
      `[HIERARCHY SKIP] status=${targetRes.status}`
    );

    return;
  }

  const targetRoles =
    Array.isArray(
      targetRes.data
    )
      ? targetRes.data
      : [];

  const targetMap =
    new Map(
      targetRoles.map(
        role => [
          role.id,
          role
        ]
      )
    );

  const payload = [];

  const sorted =
    [...sourceRoles]
      .sort(
        (a, b) =>
          Number(a.position || 0) -
          Number(b.position || 0)
      );

  for (const sourceRole of sorted) {
    const targetId =
      job.roleMap[
        sourceRole.id
      ];

    if (!targetId) {
      continue;
    }

    if (
      targetId ===
      job.targetEveryoneRoleId
    ) {
      continue;
    }

    const targetRole =
      targetMap.get(
        targetId
      );

    if (!targetRole) {
      continue;
    }

    /*
     * 봇보다 높은 역할은 이동할 수 없다.
     */
    const sourcePosition =
      Number(
        sourceRole.position || 0
      );

    if (
      sourcePosition >=
      job.botHighestRolePosition
    ) {
      console.log(
        `[HIERARCHY SKIP] role="${sourceRole.name}" sourcePosition=${sourcePosition} botPosition=${job.botHighestRolePosition}`
      );

      continue;
    }

    payload.push({
      id: targetId,
      position:
        Math.min(
          sourcePosition,
          Math.max(
            0,
            job.botHighestRolePosition - 1
          )
        )
    });
  }

  if (!payload.length) {
    return;
  }

  const res =
    await discordApi(
      `/guilds/${job.targetGuildId}/roles`,
      env.DISCORD_BOT_TOKEN,
      {
        method: 'PATCH',
        headers: {
          'Content-Type':
            'application/json'
        },
        body:
          JSON.stringify(
            payload
          )
      },
      '역할 순서 동기화'
    );

  if (!res.ok) {
    console.log(
      `[HIERARCHY SKIP] status=${res.status}`
    );
  }
}


/* ============================================================
 * Categories
 * ========================================================== */

async function processCategories(
  job,
  categories,
  env
) {
  const token =
    env.DISCORD_BOT_TOKEN;

  /*
   * 여기서는 대상 채널 전체를 딱 한 번 가져온다.
   * 기존 코드처럼 카테고리마다 GET 하지 않는다.
   */

  if (!job.targetChannelsCache) {
    const channelsRes =
      await discordApi(
        `/guilds/${job.targetGuildId}/channels`,
        token,
        {},
        '대상 채널 목록 조회'
      );

    if (!channelsRes.ok) {
      if (
        TEMPORARY_ERRORS.has(
          channelsRes.status
        )
      ) {
        return 'retry';
      }

      throw permanentError(
        `대상 채널 조회 실패 (${channelsRes.status})`
      );
    }

    job.targetChannelsCache =
      Array.isArray(
        channelsRes.data
      )
        ? channelsRes.data
        : [];
  }

  const end =
    Math.min(
      job.categoryIndex +
        MAX_CATEGORIES_PER_RUN,
      categories.length
    );

  while (
    job.categoryIndex < end
  ) {
    const source =
      categories[
        job.categoryIndex
      ];

    if (
      job.processedCategories[
        source.id
      ]
    ) {
      job.categoryIndex++;
      continue;
    }

    /*
     * 카테고리는 반드시 type=4만 비교.
     * 이름만 같다고 일반 채널을
     * 카테고리로 착각하지 않는다.
     */
    const existing =
      job.targetChannelsCache.find(
        channel =>
          Number(
            channel.type
          ) === 4 &&
          channel.name ===
            source.name
      );

    if (existing) {
      job.categoryMap[
        source.id
      ] =
        existing.id;

      job.processedCategories[
        source.id
      ] = true;

      job.categorySuccess++;

      console.log(
        `[CATEGORY REUSE] "${source.name}" -> ${existing.id}`
      );

      job.categoryIndex++;

      continue;
    }

    const body = {
      name:
        String(
          source.name || ''
        ).slice(0, 100),

      type: 4,

      permission_overwrites:
        mapPermissionOverwrites(
          source.permission_overwrites,
          job
        )
    };

    console.log(
      `[CATEGORY CREATE] name="${source.name}" body=${safeJson(body)}`
    );

    const res =
      await discordApi(
        `/guilds/${job.targetGuildId}/channels`,
        token,
        {
          method: 'POST',
          headers: {
            'Content-Type':
              'application/json'
          },
          body:
            JSON.stringify(body)
        },
        `카테고리 생성 "${source.name}"`
      );

    if (
      res.ok &&
      res.data?.id
    ) {
      job.categoryMap[
        source.id
      ] =
        res.data.id;

      job.processedCategories[
        source.id
      ] = true;

      job.categorySuccess++;

      /*
       * 캐시에 새 카테고리 추가.
       */
      job.targetChannelsCache.push(
        res.data
      );

      job.categoryIndex++;

      continue;
    }

    if (
      TEMPORARY_ERRORS.has(
        res.status
      )
    ) {
      const count =
        Number(
          job.categoryRetries[
            source.id
          ] || 0
        );

      if (
        count < MAX_RETRIES
      ) {
        job.categoryRetries[
          source.id
        ] =
          count + 1;

        return 'retry';
      }
    }

    job.categoryFailed++;

    job.processedCategories[
      source.id
    ] = true;

    console.error(
      `[CATEGORY SKIP] name=${source.name} status=${res.status} code=${res.data?.code || '-'}`
    );

    job.categoryIndex++;
  }

  return 'ok';
}


/* ============================================================
 * Channels
 * ========================================================== */

async function processChannels(
  job,
  channels,
  env
) {
  const token =
    env.DISCORD_BOT_TOKEN;

  /*
   * 대상 채널 목록은 한 번만 가져온다.
   */
  if (!job.targetChannelsCache) {
    const channelsRes =
      await discordApi(
        `/guilds/${job.targetGuildId}/channels`,
        token,
        {},
        '대상 채널 목록 조회'
      );

    if (!channelsRes.ok) {
      if (
        TEMPORARY_ERRORS.has(
          channelsRes.status
        )
      ) {
        return 'retry';
      }

      throw permanentError(
        `대상 채널 조회 실패 (${channelsRes.status})`
      );
    }

    job.targetChannelsCache =
      Array.isArray(
        channelsRes.data
      )
        ? channelsRes.data
        : [];
  }

  const end =
    Math.min(
      job.channelIndex +
        MAX_CHANNELS_PER_RUN,
      channels.length
    );

  while (
    job.channelIndex < end
  ) {
    const source =
      channels[
        job.channelIndex
      ];

    if (
      job.processedChannels[
        source.id
      ]
    ) {
      job.channelIndex++;
      continue;
    }

    /*
     * 부모 카테고리 변환.
     */
    let parentId = null;

    if (
      source.parent_id
    ) {
      parentId =
        job.categoryMap[
          source.parent_id
        ] || null;

      if (!parentId) {
        console.log(
          `[CHANNEL PARENT WARNING] channel="${source.name}" sourceParent=${source.parent_id} mapping-not-found`
        );
      }
    }

    /*
     * 지원 가능한 Discord channel type.
     */
    const sourceType =
      Number(
        source.type
      );

    let mappedType =
      sourceType;

    if (
      ![
        0, 2, 5,
        13, 14,
        15, 16,
        21
      ].includes(
        mappedType
      )
    ) {
      mappedType = 0;
    }

    /*
     * Announcement channel.
     *
     * 대상 서버에서 announcement channel을
     * 만들 수 없는 경우 400이 날 수 있으므로
     * 최초에는 type=5를 시도하고,
     * validation 실패 시 일반 text로 fallback한다.
     */

    const permissionOverwrites =
      mapPermissionOverwrites(
        source.permission_overwrites,
        job
      );

    const body =
      buildChannelBody(
        source,
        mappedType,
        parentId,
        permissionOverwrites
      );

    /*
     * 기존 채널 확인.
     *
     * 이름 + type + parent를 모두 비교한다.
     */
    const existing =
      job.targetChannelsCache.find(
        channel =>
          channel.name ===
            source.name &&
          sameChannelType(
            Number(channel.type),
            sourceType
          ) &&
          (channel.parent_id ||
            null) ===
            (parentId || null)
      );

    if (existing) {
      job.channelMap[
        source.id
      ] =
        existing.id;

      job.processedChannels[
        source.id
      ] = true;

      job.channelSuccess++;

      console.log(
        `[CHANNEL REUSE] "${source.name}" -> ${existing.id} parent=${parentId || 'none'}`
      );

      job.channelIndex++;

      continue;
    }

    console.log(
      `[CHANNEL CREATE] name="${source.name}" type=${mappedType} parent=${parentId || 'none'} body=${safeJson(body)}`
    );

    let res =
      await discordApi(
        `/guilds/${job.targetGuildId}/channels`,
        token,
        {
          method: 'POST',
          headers: {
            'Content-Type':
              'application/json'
          },
          body:
            JSON.stringify(body)
        },
        `채널 생성 "${source.name}"`
      );

    /*
     * Announcement channel 생성 실패 시
     * 일반 텍스트 채널로 fallback.
     */
    if (
      !res.ok &&
      sourceType === 5 &&
      res.status === 400 &&
      res.data?.code === 50035
    ) {
      console.log(
        `[CHANNEL FALLBACK] "${source.name}" type=5 -> type=0`
      );

      const fallbackBody =
        buildChannelBody(
          source,
          0,
          parentId,
          permissionOverwrites
        );

      res =
        await discordApi(
          `/guilds/${job.targetGuildId}/channels`,
          token,
          {
            method: 'POST',
            headers: {
              'Content-Type':
                'application/json'
            },
            body:
              JSON.stringify(
                fallbackBody
              )
          },
          `채널 생성 fallback "${source.name}"`
        );
    }

    if (
      res.ok &&
      res.data?.id
    ) {
      job.channelMap[
        source.id
      ] =
        res.data.id;

      job.processedChannels[
        source.id
      ] = true;

      job.channelSuccess++;

      job.targetChannelsCache.push(
        res.data
      );

      job.channelIndex++;

      continue;
    }

    if (
      TEMPORARY_ERRORS.has(
        res.status
      )
    ) {
      const count =
        Number(
          job.channelRetries[
            source.id
          ] || 0
        );

      if (
        count < MAX_RETRIES
      ) {
        job.channelRetries[
          source.id
        ] =
          count + 1;

        await saveJob(
          env,
          job
        );

        return 'retry';
      }
    }

    /*
     * 영구 오류.
     * 상세 Discord validation 정보는
     * discordApi에서 이미 출력된다.
     */
    job.channelFailed++;

    job.processedChannels[
      source.id
    ] = true;

    console.error(
      `[CHANNEL SKIP] name=${source.name} type=${sourceType} parent=${parentId || 'none'} status=${res.status} code=${res.data?.code || '-'}`
    );

    job.channelIndex++;
  }

  return 'ok';
}


/* ============================================================
 * Channel Body
 * ========================================================== */

function buildChannelBody(
  source,
  type,
  parentId,
  permissionOverwrites
) {
  const body = {
    name:
      String(
        source.name || ''
      ).slice(0, 100),

    type,

    permission_overwrites:
      permissionOverwrites
  };

  if (parentId) {
    body.parent_id =
      parentId;
  }

  if (
    type === 0 ||
    type === 5
  ) {
    if (
      source.topic !== null &&
      source.topic !== undefined
    ) {
      body.topic =
        String(
          source.topic
        ).slice(0, 4096);
    }

    if (
      source.nsfw !== null &&
      source.nsfw !== undefined
    ) {
      body.nsfw =
        !!source.nsfw;
    }

    if (
      source.rate_limit_per_user !==
        null &&
      source.rate_limit_per_user !==
        undefined
    ) {
      body.rate_limit_per_user =
        Number(
          source.rate_limit_per_user
        );
    }
  }

  if (
    type === 2 ||
    type === 13
  ) {
    if (
      source.bitrate !== null &&
      source.bitrate !== undefined
    ) {
      body.bitrate =
        Number(
          source.bitrate
        );
    }

    if (
      source.user_limit !== null &&
      source.user_limit !== undefined
    ) {
      body.user_limit =
        Number(
          source.user_limit
        );
    }
  }

  return body;
}


/* ============================================================
 * Channel Type Comparison
 * ========================================================== */

function sameChannelType(
  targetType,
  sourceType
) {
  if (
    targetType ===
    sourceType
  ) {
    return true;
  }

  /*
   * Announcement -> Text fallback을
   * 기존 채널로 인식할 수 있도록 한다.
   */
  if (
    sourceType === 5 &&
    targetType === 0
  ) {
    return true;
  }

  return false;
}


/* ============================================================
 * Permission Mapping
 * ========================================================== */

function mapPermissionOverwrites(
  overwrites,
  job
) {
  if (
    !Array.isArray(
      overwrites
    )
  ) {
    return [];
  }

  return overwrites
    .filter(
      ow =>
        Number(ow.type) === 0
    )
    .map(
      ow => {
        let mappedId = null;

        /*
         * @everyone
         */
        if (
          ow.id ===
          job.sourceGuildId
        ) {
          mappedId =
            job.targetEveryoneRoleId;
        }

        /*
         * Role
         */
        else if (
          job.roleMap &&
          job.roleMap[
            ow.id
          ]
        ) {
          mappedId =
            job.roleMap[
              ow.id
            ];
        }

        if (!mappedId) {
          return null;
        }

        return {
          id:
            mappedId,

          type: 0,

          allow:
            String(
              ow.allow || '0'
            ),

          deny:
            String(
              ow.deny || '0'
            )
        };
      }
    )
    .filter(Boolean);
}


/* ============================================================
 * Channel Position Synchronization
 * ========================================================== */

async function synchronizeChannelPositions(
  job,
  categories,
  channels,
  env
) {
  /*
   * 중요:
   *
   * 원본 position 숫자를 무조건 대상 서버 전체에
   * 그대로 박아 넣지 않는다.
   *
   * 먼저 카테고리를 정렬하고,
   * 각 카테고리 내부 채널을 정렬한다.
   */

  const categoryPayload = [];

  for (
    const category of categories
  ) {
    const targetId =
      job.categoryMap?.[
        category.id
      ];

    if (!targetId) {
      continue;
    }

    categoryPayload.push({
      id:
        targetId,

      position:
        Number(
          category.position || 0
        )
    });
  }

  /*
   * 카테고리 순서.
   */
  if (
    categoryPayload.length
  ) {
    const categoryRes =
      await discordApi(
        `/guilds/${job.targetGuildId}/channels`,
        env.DISCORD_BOT_TOKEN,
        {
          method: 'PATCH',
          headers: {
            'Content-Type':
              'application/json'
          },
          body:
            JSON.stringify(
              categoryPayload
            )
        },
        '카테고리 순서 동기화'
      );

    if (!categoryRes.ok) {
      console.error(
        `[CATEGORY POSITION SKIP] status=${categoryRes.status} code=${categoryRes.data?.code || '-'}`
      );
    }
  }

  /*
   * 일반 채널은 parent별로 묶는다.
   */
  const groups =
    new Map();

  for (
    const source of channels
  ) {
    const targetId =
      job.channelMap?.[
        source.id
      ];

    if (!targetId) {
      continue;
    }

    const targetParent =
      source.parent_id
        ? job.categoryMap?.[
            source.parent_id
          ] || null
        : null;

    const key =
      targetParent ||
      '__NO_PARENT__';

    if (
      !groups.has(key)
    ) {
      groups.set(
        key,
        []
      );
    }

    groups
      .get(key)
      .push({
        source,
        targetId
      });
  }

  /*
   * 카테고리별로 순서를 별도 PATCH.
   */
  for (
    const [
      parentId,
      items
    ] of groups
  ) {
    items.sort(
      (a, b) =>
        Number(
          a.source.position || 0
        ) -
        Number(
          b.source.position || 0
        )
    );

    const payload =
      items.map(
        item => ({
          id:
            item.targetId,

          /*
           * 같은 parent 안에서 0부터
           * 상대적인 순서를 사용.
           */
          position:
            items.indexOf(
              item
            )
        })
      );

    if (
      !payload.length
    ) {
      continue;
    }

    const res =
      await discordApi(
        `/guilds/${job.targetGuildId}/channels`,
        env.DISCORD_BOT_TOKEN,
        {
          method: 'PATCH',
          headers: {
            'Content-Type':
              'application/json'
          },
          body:
            JSON.stringify(
              payload
            )
        },
        `채널 순서 동기화 parent=${parentId}`
      );

    if (!res.ok) {
      console.error(
        `[CHANNEL POSITION SKIP] parent=${parentId} status=${res.status} code=${res.data?.code || '-'}`
      );
    }
  }
}


/* ============================================================
 * Progress
 * ========================================================== */

function createProgressUpdater(
  env,
  job
) {
  return async (
    content,
    force = false
  ) => {
    const now =
      Date.now();

    if (
      !force &&
      content ===
        job.lastProgress
    ) {
      return;
    }

    if (
      !force &&
      now -
        job.lastProgressAt <
        PROGRESS_MIN_INTERVAL_MS
    ) {
      return;
    }

    job.lastProgress =
      content;

    job.lastProgressAt =
      now;

    await saveJob(
      env,
      job
    );

    await updateProgress(
      job.applicationId,
      job.interactionToken,
      content,
      force
    );
  };
}


async function updateProgress(
  applicationId,
  interactionToken,
  content,
  force = false
) {
  if (
    !applicationId ||
    !interactionToken
  ) {
    return false;
  }

  const url =
    `${DISCORD_API}/webhooks/` +
    `${applicationId}/` +
    `${interactionToken}/` +
    `messages/@original`;

  try {
    const res =
      await fetch(
        url,
        {
          method: 'PATCH',

          headers: {
            'Content-Type':
              'application/json'
          },

          body:
            JSON.stringify({
              content,

              allowed_mentions: {
                parse: []
              }
            })
        }
      );

    if (!res.ok) {
      const text =
        await res.text();

      console.error(
        `[PROGRESS ERROR] status=${res.status} response=${text.slice(0, 500)}`
      );

      return false;
    }

    return true;

  } catch (err) {
    console.error(
      '[PROGRESS FETCH ERROR]',
      err?.message || err
    );

    return false;
  }
}


/* ============================================================
 * Progress Builder
 * ========================================================== */

function buildProgress(
  job,
  roles,
  categories,
  channels
) {
  let phaseText =
    '⏳ 처리 중...';

  if (
    job.phase ===
    'ROLES'
  ) {
    phaseText =
      '👤 역할을 생성하는 중...';
  }

  if (
    job.phase ===
    'ROLES_SYNC'
  ) {
    phaseText =
      '🔃 역할 순서를 동기화하는 중...';
  }

  if (
    job.phase ===
    'CATEGORIES'
  ) {
    phaseText =
      '📁 카테고리를 생성하는 중...';
  }

  if (
    job.phase ===
    'CHANNELS'
  ) {
    phaseText =
      '💬 채널을 생성하는 중...';
  }

  if (
    job.phase ===
    'CHANNEL_SYNC'
  ) {
    phaseText =
      '🔃 채널 순서를 동기화하는 중...';
  }

  return (
    `🚀 **서버 복제 진행 중**\n\n` +

    `👤 역할: ` +
    `${job.roleSuccess}/${roles.length}\n` +

    `🔃 역할 순서: ` +
    `${
      job.phase === 'ROLES_SYNC' ||
      job.phase === 'CATEGORIES' ||
      job.phase === 'CHANNELS' ||
      job.phase === 'CHANNEL_SYNC'
        ? '처리 완료'
        : '처리 중'
    }\n` +

    `📁 카테고리: ` +
    `${job.categorySuccess}/${categories.length}\n` +

    `💬 채널: ` +
    `${job.channelSuccess}/${channels.length}\n\n` +

    `${phaseText}`
  );
}


/* ============================================================
 * Queue
 * ========================================================== */

async function enqueueNext(
  env,
  job
) {
  if (
    !env.CLONE_QUEUE
  ) {
    throw new Error(
      'CLONE_QUEUE binding missing'
    );
  }

  console.log(
    `[QUEUE NEXT] job=${job.jobId} phase=${job.phase}`
  );

  await env.CLONE_QUEUE.send({
    jobId:
      job.jobId
  });
}


/* ============================================================
 * KV
 * ========================================================== */

async function saveJob(
  env,
  job
) {
  await env.SERVER_BACKUPS.put(
    job.jobId,
    JSON.stringify(job)
  );
}


/* ============================================================
 * Discord API
 * ========================================================== */

async function discordApi(
  path,
  token,
  options = {},
  label = 'Discord API'
) {
  const url =
    `${DISCORD_API}${path}`;

  const maxAttempts = 3;

  for (
    let attempt = 1;
    attempt <= maxAttempts;
    attempt++
  ) {
    try {
      const res =
        await fetch(
          url,
          {
            ...options,

            headers: {
              Authorization:
                `Bot ${token}`,

              ...(options.headers ||
                {})
            }
          }
        );

      const text =
        await res.text();

      let data = null;

      if (text) {
        try {
          data =
            JSON.parse(
              text
            );
        } catch {
          data = null;
        }
      }

      /*
       * 429
       */
      if (
        res.status ===
        429
      ) {
        const retryAfter =
          Number(
            data?.retry_after ??
            res.headers.get(
              'Retry-After'
            ) ??
            1
          );

        console.warn(
          `[DISCORD 429] ${label} retryAfter=${retryAfter}s attempt=${attempt}`
        );

        if (
          retryAfter > 5
        ) {
          return {
            ok: false,
            status: 429,
            statusText:
              res.statusText,
            data,
            text
          };
        }

        await sleep(
          retryAfter *
            1000
        );

        continue;
      }

      /*
       * 5xx / temporary
       */
      if (
        TEMPORARY_ERRORS.has(
          res.status
        )
      ) {
        console.warn(
          `[DISCORD TEMP] ${label} method=${options.method || 'GET'} status=${res.status} attempt=${attempt}`
        );

        if (
          attempt <
          maxAttempts
        ) {
          await sleep(
            attempt *
              1000
          );

          continue;
        }
      }

      if (!res.ok) {
        console.error(
          `[DISCORD ERROR] ${label} method=${options.method || 'GET'} status=${res.status} code=${data?.code || '-'} message=${data?.message || res.statusText || '-'}`
        );

        /*
         * 50035의 진짜 원인을
         * 반드시 로그로 남긴다.
         */
        if (
          res.status ===
            400 &&
          data?.code ===
            50035
        ) {
          console.error(
            `[DISCORD VALIDATION ERROR] ${label} errors=${safeJson(data?.errors || {})}`
          );
        }

        /*
         * Request Body도 출력.
         * Token은 options에 없으므로
         * Authorization은 출력하지 않는다.
         */
        if (
          options.body
        ) {
          console.error(
            `[DISCORD REQUEST BODY] ${label} body=${String(options.body).slice(0, 3000)}`
          );
        }
      } else {
        console.log(
          `[DISCORD OK] ${label} method=${options.method || 'GET'} status=${res.status}`
        );
      }

      return {
        ok:
          res.ok,

        status:
          res.status,

        statusText:
          res.statusText,

        data,

        text
      };

    } catch (err) {
      console.error(
        `[DISCORD FETCH ERROR] ${label} attempt=${attempt}`,
        err?.stack || err
      );

      if (
        attempt <
        maxAttempts
      ) {
        await sleep(
          attempt *
            1000
        );

        continue;
      }

      return {
        ok: false,
        status: 0,
        statusText: '',
        data: null,
        text:
          errorMessage(err)
      };
    }
  }

  return {
    ok: false,
    status: 503,
    statusText:
      'Maximum attempts reached',
    data: null,
    text:
      'Maximum attempts reached'
  };
}


/* ============================================================
 * Discord Signature
 * ========================================================== */

async function verifyDiscordRequest(
  signature,
  timestamp,
  body,
  publicKey
) {
  if (
    !signature ||
    !timestamp ||
    !body ||
    !publicKey
  ) {
    return false;
  }

  try {
    const hexToBytes =
      hex => {
        if (
          typeof hex !==
            'string' ||
          hex.length % 2 !==
            0
        ) {
          throw new Error(
            'Invalid hex'
          );
        }

        const bytes =
          new Uint8Array(
            hex.length / 2
          );

        for (
          let i = 0;
          i <
            bytes.length;
          i++
        ) {
          bytes[i] =
            parseInt(
              hex.slice(
                i * 2,
                i * 2 + 2
              ),
              16
            );
        }

        return bytes;
      };

    const publicKeyBytes =
      hexToBytes(
        publicKey
      );

    const signatureBytes =
      hexToBytes(
        signature
      );

    const messageBytes =
      new TextEncoder().encode(
        timestamp +
          body
      );

    const key =
      await crypto.subtle.importKey(
        'raw',
        publicKeyBytes,
        {
          name:
            'Ed25519'
        },
        false,
        ['verify']
      );

    return await crypto.subtle.verify(
      {
        name:
          'Ed25519'
      },
      key,
      signatureBytes,
      messageBytes
    );

  } catch (err) {
    console.error(
      '[VERIFY ERROR]',
      err?.message || err
    );

    return false;
  }
}


/* ============================================================
 * Slash Registration
 * ========================================================== */

async function registerSlashCommands(
  token,
  applicationId
) {
  if (
    !token ||
    !applicationId
  ) {
    return {
      status: 500,
      text:
        'DISCORD_BOT_TOKEN 또는 DISCORD_APPLICATION_ID가 없습니다.'
    };
  }

  const commands = [
    {
      name:
        'serverid',

      description:
        '현재 서버의 ID를 확인합니다.'
    },

    {
      name:
        'channelid',

      description:
        '현재 채널의 ID를 확인합니다.'
    },

    {
      name:
        'backup',

      description:
        '서버의 역할과 채널 구조를 백업합니다.',

      options: [
        {
          type: 3,

          name:
            'guild_id',

          description:
            '백업할 서버 ID',

          required:
            false
        }
      ]
    },

    {
      name:
        'clone',

      description:
        '백업된 서버 구조를 다른 서버에 복제합니다.',

      options: [
        {
          type: 3,

          name:
            'source_guild_id',

          description:
            '원본 서버 ID',

          required:
            true
        },

        {
          type: 3,

          name:
            'target_guild_id',

          description:
            '대상 서버 ID',

          required:
            true
        }
      ]
    }
  ];

  const url =
    `${DISCORD_API}/applications/` +
    `${applicationId}/commands`;

  const res =
    await fetch(
      url,
      {
        method:
          'PUT',

        headers: {
          Authorization:
            `Bot ${token}`,

          'Content-Type':
            'application/json'
        },

        body:
          JSON.stringify(
            commands
          )
      }
    );

  const text =
    await res.text();

  if (res.ok) {
    return {
      status: 200,
      text:
        '슬래시 명령어 등록 완료!'
    };
  }

  return {
    status:
      res.status,

    text:
      `명령어 등록 실패:\n${text}`
  };
}


/* ============================================================
 * Utilities
 * ========================================================== */

function json(
  data,
  status = 200
) {
  return new Response(
    JSON.stringify(
      data
    ),
    {
      status,

      headers: {
        'Content-Type':
          'application/json; charset=utf-8',

        'Cache-Control':
          'no-store'
      }
    }
  );
}


function sleep(
  ms
) {
  return new Promise(
    resolve =>
      setTimeout(
        resolve,
        ms
      )
  );
}


/*
 * TypeScript/VS Code에서
 * Error.permanent 오류가 나지 않도록
 * Error 객체에 직접 접근하지 않는다.
 */
function permanentError(
  message
) {
  const err =
    new Error(
      message
    );

  Object.defineProperty(
    err,
    '__permanent',
    {
      value: true,
      enumerable: false,
      configurable: true
    }
  );

  return err;
}


function isPermanentError(
  err
) {
  return (
    !!err &&
    err.__permanent ===
      true
  );
}


function errorMessage(
  err
) {
  if (
    err instanceof Error
  ) {
    return (
      err.message ||
      String(err)
    );
  }

  return String(err);
}


function escapeDiscordText(
  text
) {
  return String(text)
    .replace(
      /\\/g,
      '\\\\'
    )
    .replace(
      /`/g,
      '\\`'
    );
}


function safeJson(
  value
) {
  try {
    return JSON.stringify(
      value
    );
  } catch {
    return '{}';
  }
}
/* ============================================================
 * 2/2 — Remaining Logic / Validation / Utilities
 * ========================================================== */


/* ============================================================
 * Category Validation
 *
 * 카테고리가 실제로 생성/매핑되었는지 확인한다.
 * ========================================================== */

async function validateCategoryMappings(job, categories, env) {
  if (!Array.isArray(categories) || categories.length === 0) {
    return true;
  }

  const res = await discordApi(
    `/guilds/${job.targetGuildId}/channels`,
    env.DISCORD_BOT_TOKEN,
    {},
    '카테고리 매핑 검증'
  );

  if (!res.ok) {
    console.error(
      `[CATEGORY VALIDATION ERROR] status=${res.status}`
    );

    return false;
  }

  const targetChannels =
    Array.isArray(res.data)
      ? res.data
      : [];

  const targetMap = new Map(
    targetChannels.map(channel => [
      channel.id,
      channel
    ])
  );

  let validCount = 0;

  for (const category of categories) {
    const targetId =
      job.categoryMap?.[category.id];

    if (!targetId) {
      console.error(
        `[CATEGORY MISSING] source=${category.name} sourceId=${category.id}`
      );
      continue;
    }

    const target =
      targetMap.get(targetId);

    if (!target) {
      console.error(
        `[CATEGORY TARGET MISSING] source=${category.name} targetId=${targetId}`
      );
      continue;
    }

    if (Number(target.type) !== 4) {
      console.error(
        `[CATEGORY TYPE ERROR] source=${category.name} targetType=${target.type}`
      );
      continue;
    }

    validCount++;
  }

  console.log(
    `[CATEGORY VALIDATION] ${validCount}/${categories.length}`
  );

  return validCount === categories.length;
}


/* ============================================================
 * Safe Category Parent Resolution
 *
 * 채널 생성 전에 parent_id가 진짜 카테고리인지 확인한다.
 * 잘못된 parent_id를 넣어서 채널이 엉뚱한 곳으로 들어가는
 * 문제를 방지한다.
 * ========================================================== */

async function resolveCategoryParent(
  job,
  channel,
  env
) {
  if (!channel.parent_id) {
    return null;
  }

  const mappedId =
    job.categoryMap?.[channel.parent_id];

  if (!mappedId) {
    console.warn(
      `[PARENT MAP MISSING] channel=${channel.name} sourceParent=${channel.parent_id}`
    );

    return null;
  }

  /*
   * 이미 검증된 카테고리라면 바로 사용.
   */
  if (
    job.validatedCategories?.[channel.parent_id] === mappedId
  ) {
    return mappedId;
  }

  const res = await discordApi(
    `/guilds/${job.targetGuildId}/channels`,
    env.DISCORD_BOT_TOKEN,
    {},
    `부모 카테고리 확인 "${channel.name}"`
  );

  if (!res.ok) {
    console.error(
      `[PARENT CHECK ERROR] channel=${channel.name} status=${res.status}`
    );

    return null;
  }

  const channels =
    Array.isArray(res.data)
      ? res.data
      : [];

  const parent =
    channels.find(
      x => x.id === mappedId
    );

  if (!parent) {
    console.error(
      `[PARENT NOT FOUND] channel=${channel.name} parent=${mappedId}`
    );

    return null;
  }

  /*
   * 반드시 Category(type 4)여야 한다.
   */
  if (Number(parent.type) !== 4) {
    console.error(
      `[PARENT NOT_CATEGORY] channel=${channel.name} parent=${mappedId} type=${parent.type}`
    );

    return null;
  }

  if (!job.validatedCategories) {
    job.validatedCategories = {};
  }

  job.validatedCategories[channel.parent_id] =
    mappedId;

  return mappedId;
}


/* ============================================================
 * Channel Creation Body
 *
 * Discord API에 들어갈 수 있는 필드만 명확하게 구성한다.
 * ========================================================== */

function buildChannelCreateBody(
  channel,
  parentId
) {
  const sourceType =
    Number(channel.type);

  /*
   * Discord에서 복제가 가능한 기본 타입.
   *
   * 0  text
   * 2  voice
   * 4  category
   * 5  announcement
   * 13 stage
   * 14 forum
   */
  let type = sourceType;

  const supportedTypes = [
    0,
    2,
    4,
    5,
    13,
    14
  ];

  if (!supportedTypes.includes(type)) {
    console.warn(
      `[CHANNEL TYPE FALLBACK] name=${channel.name} sourceType=${sourceType} -> text`
    );

    type = 0;
  }

  /*
   * Category는 별도 처리.
   */
  if (type === 4) {
    return {
      name: String(channel.name || 'category')
        .slice(0, 100),

      type: 4,

      permission_overwrites:
        normalizePermissionOverwrites(
          channel.permission_overwrites
        )
    };
  }

  const body = {
    name: String(channel.name || 'channel')
      .slice(0, 100),

    type,

    permission_overwrites:
      normalizePermissionOverwrites(
        channel.permission_overwrites
      )
  };

  /*
   * 부모 카테고리는 유효한 경우에만 넣는다.
   */
  if (parentId) {
    body.parent_id = parentId;
  }

  /*
   * Text
   */
  if (type === 0) {
    if (
      channel.topic !== null &&
      channel.topic !== undefined
    ) {
      body.topic =
        String(channel.topic).slice(0, 4096);
    }

    if (
      channel.nsfw !== null &&
      channel.nsfw !== undefined
    ) {
      body.nsfw =
        Boolean(channel.nsfw);
    }

    if (
      channel.rate_limit_per_user !== null &&
      channel.rate_limit_per_user !== undefined
    ) {
      body.rate_limit_per_user =
        Math.max(
          0,
          Math.min(
            21600,
            Number(channel.rate_limit_per_user) || 0
          )
        );
    }
  }

  /*
   * Announcement
   */
  if (type === 5) {
    if (
      channel.topic !== null &&
      channel.topic !== undefined
    ) {
      body.topic =
        String(channel.topic).slice(0, 4096);
    }

    if (
      channel.nsfw !== null &&
      channel.nsfw !== undefined
    ) {
      body.nsfw =
        Boolean(channel.nsfw);
    }
  }

  /*
   * Voice
   */
  if (type === 2 || type === 13) {
    if (
      channel.bitrate !== null &&
      channel.bitrate !== undefined
    ) {
      const bitrate =
        Number(channel.bitrate);

      if (
        Number.isFinite(bitrate) &&
        bitrate > 0
      ) {
        body.bitrate = bitrate;
      }
    }

    if (
      channel.user_limit !== null &&
      channel.user_limit !== undefined
    ) {
      const userLimit =
        Number(channel.user_limit);

      if (
        Number.isFinite(userLimit) &&
        userLimit >= 0
      ) {
        body.user_limit =
          userLimit;
      }
    }
  }

  /*
   * Forum
   *
   * Discord API에서 원본 forum의 모든 부가 필드를
   * 그대로 넣으면 400이 발생할 수 있으므로
   * 최소 필드만 사용한다.
   */
  if (type === 14) {
    if (
      channel.topic !== null &&
      channel.topic !== undefined
    ) {
      body.topic =
        String(channel.topic).slice(0, 4096);
    }

    if (
      channel.nsfw !== null &&
      channel.nsfw !== undefined
    ) {
      body.nsfw =
        Boolean(channel.nsfw);
    }
  }

  return body;
}


/* ============================================================
 * Permission Overwrite Normalizer
 * ========================================================== */

function normalizePermissionOverwrites(
  overwrites
) {
  if (!Array.isArray(overwrites)) {
    return [];
  }

  return overwrites
    .filter(
      overwrite =>
        overwrite &&
        overwrite.id
    )
    .map(overwrite => {
      const result = {
        id: String(overwrite.id),

        type:
          Number(overwrite.type) === 1
            ? 1
            : 0,

        allow:
          String(
            overwrite.allow ?? '0'
          ),

        deny:
          String(
            overwrite.deny ?? '0'
          )
      };

      return result;
    });
}


/* ============================================================
 * Improved Permission Mapping
 * ========================================================== */

function mapPermissionOverwritesSafe(
  overwrites,
  job
) {
  if (!Array.isArray(overwrites)) {
    return [];
  }

  const result = [];

  for (const overwrite of overwrites) {
    if (
      !overwrite ||
      !overwrite.id
    ) {
      continue;
    }

    /*
     * User overwrite는 복제할 수 없으므로 제외.
     *
     * role overwrite만 처리한다.
     */
    if (
      Number(overwrite.type) !== 0
    ) {
      continue;
    }

    let targetId = null;

    /*
     * @everyone
     */
    if (
      overwrite.id ===
      job.sourceGuildId
    ) {
      targetId =
        job.targetEveryoneRoleId;
    }

    /*
     * Role
     */
    else if (
      job.roleMap?.[overwrite.id]
    ) {
      targetId =
        job.roleMap[overwrite.id];
    }

    if (!targetId) {
      console.warn(
        `[OVERWRITE SKIP] sourceId=${overwrite.id}`
      );

      continue;
    }

    result.push({
      id: targetId,
      type: 0,

      allow:
        String(
          overwrite.allow ?? '0'
        ),

      deny:
        String(
          overwrite.deny ?? '0'
        )
    });
  }

  return result;
}


/* ============================================================
 * Rebuild Category Map
 *
 * Queue retry / Worker 재시작 후에도 실제 서버를 확인해서
 * categoryMap을 복구할 수 있게 한다.
 * ========================================================== */

async function rebuildCategoryMap(
  job,
  categories,
  env
) {
  const res =
    await discordApi(
      `/guilds/${job.targetGuildId}/channels`,
      env.DISCORD_BOT_TOKEN,
      {},
      '카테고리 맵 복구'
    );

  if (!res.ok) {
    return;
  }

  const targetChannels =
    Array.isArray(res.data)
      ? res.data
      : [];

  const targetCategories =
    targetChannels.filter(
      x => Number(x.type) === 4
    );

  if (!job.categoryMap) {
    job.categoryMap = {};
  }

  for (const sourceCategory of categories) {
    /*
     * 이미 매핑되어 있고 실제 대상에 존재하면 유지.
     */
    const currentId =
      job.categoryMap[sourceCategory.id];

    if (
      currentId &&
      targetCategories.some(
        x => x.id === currentId
      )
    ) {
      continue;
    }

    /*
     * 이름 기준 fallback.
     */
    const found =
      targetCategories.find(
        x =>
          x.name === sourceCategory.name
      );

    if (found) {
      job.categoryMap[sourceCategory.id] =
        found.id;

      job.processedCategories[sourceCategory.id] =
        true;

      console.log(
        `[CATEGORY MAP RECOVERED] ${sourceCategory.name} -> ${found.id}`
      );
    }
  }
}


/* ============================================================
 * Rebuild Channel Map
 * ========================================================== */

async function rebuildChannelMap(
  job,
  channels,
  env
) {
  const res =
    await discordApi(
      `/guilds/${job.targetGuildId}/channels`,
      env.DISCORD_BOT_TOKEN,
      {},
      '채널 맵 복구'
    );

  if (!res.ok) {
    return;
  }

  const targetChannels =
    Array.isArray(res.data)
      ? res.data
      : [];

  if (!job.channelMap) {
    job.channelMap = {};
  }

  for (const source of channels) {
    const mapped =
      job.channelMap[source.id];

    if (
      mapped &&
      targetChannels.some(
        x => x.id === mapped
      )
    ) {
      continue;
    }

    const targetParent =
      source.parent_id
        ? job.categoryMap?.[source.parent_id] || null
        : null;

    const found =
      targetChannels.find(
        x =>
          x.name === source.name &&
          Number(x.type) === Number(source.type) &&
          (x.parent_id || null) ===
            targetParent
      );

    if (found) {
      job.channelMap[source.id] =
        found.id;

      job.processedChannels[source.id] =
        true;

      console.log(
        `[CHANNEL MAP RECOVERED] ${source.name} -> ${found.id}`
      );
    }
  }
}


/* ============================================================
 * Final Structure Validation
 * ========================================================== */

async function validateFinalStructure(
  job,
  roles,
  categories,
  channels,
  env
) {
  const res =
    await discordApi(
      `/guilds/${job.targetGuildId}/channels`,
      env.DISCORD_BOT_TOKEN,
      {},
      '최종 구조 확인'
    );

  if (!res.ok) {
    console.error(
      `[FINAL VALIDATION ERROR] status=${res.status}`
    );

    return {
      categories: 0,
      channels: 0
    };
  }

  const targetChannels =
    Array.isArray(res.data)
      ? res.data
      : [];

  let categoryCount = 0;
  let channelCount = 0;

  /*
   * Category
   */
  for (const category of categories) {
    const targetId =
      job.categoryMap?.[category.id];

    if (!targetId) {
      continue;
    }

    const target =
      targetChannels.find(
        x => x.id === targetId
      );

    if (
      target &&
      Number(target.type) === 4
    ) {
      categoryCount++;
    }
  }

  /*
   * Channel
   */
  for (const channel of channels) {
    const targetId =
      job.channelMap?.[channel.id];

    if (!targetId) {
      continue;
    }

    const target =
      targetChannels.find(
        x => x.id === targetId
      );

    if (!target) {
      continue;
    }

    const expectedParent =
      channel.parent_id
        ? job.categoryMap?.[channel.parent_id] || null
        : null;

    if (
      (target.parent_id || null) ===
      expectedParent
    ) {
      channelCount++;
    }
  }

  console.log(
    `[FINAL VALIDATION] categories=${categoryCount}/${categories.length} channels=${channelCount}/${channels.length}`
  );

  return {
    categories: categoryCount,
    channels: channelCount
  };
}


/* ============================================================
 * Improved Channel Position Sync
 *
 * 카테고리 position과 일반 채널 position을 섞어서
 * 잘못된 위치가 만들어지는 것을 방지한다.
 * ========================================================== */

async function synchronizeAllChannelPositions(
  job,
  categories,
  channels,
  env
) {
  const payload = [];

  /*
   * Category
   */
  for (const category of categories) {
    const targetId =
      job.categoryMap?.[category.id];

    if (!targetId) {
      continue;
    }

    payload.push({
      id: targetId,

      position:
        Number(category.position || 0)
    });
  }

  /*
   * Normal channels
   */
  for (const channel of channels) {
    const targetId =
      job.channelMap?.[channel.id];

    if (!targetId) {
      continue;
    }

    payload.push({
      id: targetId,

      position:
        Number(channel.position || 0)
    });
  }

  if (!payload.length) {
    return true;
  }

  const res =
    await discordApi(
      `/guilds/${job.targetGuildId}/channels`,
      env.DISCORD_BOT_TOKEN,
      {
        method: 'PATCH',

        headers: {
          'Content-Type':
            'application/json'
        },

        body:
          JSON.stringify(payload)
      },
      '전체 채널 위치 동기화'
    );

  if (!res.ok) {
    console.error(
      `[POSITION SYNC ERROR] status=${res.status} response=${JSON.stringify(res.data).slice(0, 500)}`
    );

    return false;
  }

  return true;
}


/* ============================================================
 * Discord API Error Helper
 * ========================================================== */

function getDiscordErrorMessage(
  response
) {
  if (!response) {
    return 'Discord API 응답 없음';
  }

  if (
    response.data?.message
  ) {
    return String(
      response.data.message
    );
  }

  if (
    response.data?.errors
  ) {
    try {
      return JSON.stringify(
        response.data.errors
      ).slice(0, 1000);
    } catch {
      return 'Invalid Form Body';
    }
  }

  if (response.text) {
    return String(
      response.text
    ).slice(0, 1000);
  }

  return `HTTP ${response.status}`;
}


/* ============================================================
 * Detailed Discord API Error Logging
 * ========================================================== */

function logDiscordValidationError(
  label,
  response,
  body
) {
  if (
    response?.status !== 400
  ) {
    return;
  }

  console.error(
    `[DISCORD 400 DETAIL] ${label}`
  );

  console.error(
    `[DISCORD 400 BODY] ${JSON.stringify(body)}`
  );

  console.error(
    `[DISCORD 400 RESPONSE] ${JSON.stringify(response.data)}`
  );
}


/* ============================================================
 * Job Progress Percentage
 * ========================================================== */

function calculateProgress(
  job,
  roles,
  categories,
  channels
) {
  const total =
    roles.length +
    categories.length +
    channels.length;

  if (total === 0) {
    return 100;
  }

  const completed =
    job.roleSuccess +
    job.categorySuccess +
    job.channelSuccess;

  return Math.min(
    100,
    Math.round(
      (completed / total) * 100
    )
  );
}


/* ============================================================
 * Better Progress Builder
 * ========================================================== */

function buildDetailedProgress(
  job,
  roles,
  categories,
  channels
) {
  const progress =
    calculateProgress(
      job,
      roles,
      categories,
      channels
    );

  let phase =
    '초기화';

  switch (job.phase) {
    case 'ROLES':
      phase = '역할 생성';
      break;

    case 'ROLES_SYNC':
      phase = '역할 순서 동기화';
      break;

    case 'CATEGORIES':
      phase = '카테고리 생성';
      break;

    case 'CHANNELS':
      phase = '채널 생성';
      break;

    case 'CHANNEL_SYNC':
      phase = '채널 순서 동기화';
      break;

    case 'COMPLETED':
      phase = '완료';
      break;
  }

  return (
    `🚀 **서버 복제 진행 중**\n\n` +

    `📊 진행률: **${progress}%**\n` +
    `⚙️ 현재 작업: **${phase}**\n\n` +

    `👤 역할: ` +
    `${job.roleSuccess}/${roles.length}` +
    ` (실패 ${job.roleFailed})\n` +

    `📁 카테고리: ` +
    `${job.categorySuccess}/${categories.length}` +
    ` (실패 ${job.categoryFailed})\n` +

    `💬 채널: ` +
    `${job.channelSuccess}/${channels.length}` +
    ` (실패 ${job.channelFailed})`
  );
}


/* ============================================================
 * Safe JSON Parse
 * ========================================================== */

function safeJsonParse(
  value,
  fallback = null
) {
  try {
    return JSON.parse(value);
  } catch {
    return fallback;
  }
}


/* ============================================================
 * ID Validation
 * ========================================================== */

function isDiscordSnowflake(
  value
) {
  return (
    typeof value === 'string' &&
    /^\d{15,25}$/.test(value)
  );
}


/* ============================================================
 * Sanitize Channel Name
 * ========================================================== */

function sanitizeChannelName(
  name
) {
  let value =
    String(name || 'channel')
      .trim();

  if (!value) {
    value = 'channel';
  }

  return value.slice(0, 100);
}


/* ============================================================
 * Sanitize Category Name
 * ========================================================== */

function sanitizeCategoryName(
  name
) {
  let value =
    String(name || 'category')
      .trim();

  if (!value) {
    value = 'category';
  }

  return value.slice(0, 100);
}


/* ============================================================
 * Error Type
 *
 * TS2339:
 * Property 'permanent' does not exist on type 'Error'
 *
 * 이 문제를 없애기 위해 Error를 직접 확장한다.
 * ========================================================== */

class PermanentCloneError extends Error {
  constructor(message) {
    super(message);

    this.name =
      'PermanentCloneError';

    this.permanent = true;
  }
}


/* ============================================================
 * Temporary Clone Error
 * ========================================================== */

class TemporaryCloneError extends Error {
  constructor(message) {
    super(message);

    this.name =
      'TemporaryCloneError';

    this.permanent = false;
  }
}


/* ============================================================
 * Safe Permanent Error
 *
 * 기존 permanentError()를 이것으로 교체.
 * ========================================================== */

function createPermanentError(
  message
) {
  return new PermanentCloneError(
    message
  );
}


/* ============================================================
 * Safe Temporary Error
 * ========================================================== */

function createTemporaryError(
  message
) {
  return new TemporaryCloneError(
    message
  );
}


/* ============================================================
 * Error Classification
 * ========================================================== */

function isPermanentError(
  error
) {
  return (
    error instanceof PermanentCloneError ||
    (
      error &&
      typeof error === 'object' &&
      error.permanent === true
    )
  );
}


/* ============================================================
 * Queue Retry Delay
 * ========================================================== */

function getRetryDelay(
  attempt = 1
) {
  const safeAttempt =
    Math.max(
      1,
      Number(attempt) || 1
    );

  /*
   * 2s → 4s → 8s → 최대 30s
   */
  return Math.min(
    30000,
    2000 *
      Math.pow(
        2,
        safeAttempt - 1
      )
  );
}


/* ============================================================
 * Timestamp
 * ========================================================== */

function nowIso() {
  return new Date().toISOString();
}


/* ============================================================
 * Final Debug Snapshot
 * ========================================================== */

function createJobSnapshot(
  job
) {
  return {
    jobId: job.jobId,

    phase: job.phase,

    status: job.status,

    roleIndex:
      job.roleIndex,

    categoryIndex:
      job.categoryIndex,

    channelIndex:
      job.channelIndex,

    roleSuccess:
      job.roleSuccess,

    roleFailed:
      job.roleFailed,

    categorySuccess:
      job.categorySuccess,

    categoryFailed:
      job.categoryFailed,

    channelSuccess:
      job.channelSuccess,

    channelFailed:
      job.channelFailed,

    roleMappings:
      Object.keys(
        job.roleMap || {}
      ).length,

    categoryMappings:
      Object.keys(
        job.categoryMap || {}
      ).length,

    channelMappings:
      Object.keys(
        job.channelMap || {}
      ).length,

    updatedAt:
      nowIso()
  };
}


/* ============================================================
 * END OF FILE
 * ========================================================== */
