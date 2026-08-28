/**
 * Discord Server Cloner Worker
 * Cloudflare Workers + Queues + KV
 *
 * Commands:
 * /serverid
 * /channelid
 * /backup [guild_id]
 * /clone source_guild_id target_guild_id
 *
 * Required bindings:
 * - SERVER_BACKUPS : KV
 * - CLONE_QUEUE     : Queue
 *
 * Required vars:
 * - DISCORD_BOT_TOKEN
 * - DISCORD_APPLICATION_ID
 * - DISCORD_PUBLIC_KEY
 */

const DISCORD_API = 'https://discord.com/api/v10';

const MAX_CHANNELS_PER_RUN = 5;
const MAX_CATEGORIES_PER_RUN = 5;
const MAX_ROLES_PER_RUN = 5;

const JOB_LOCK_MS = 55000;
const PROGRESS_MIN_INTERVAL_MS = 1200;

const TEMPORARY_ERRORS = new Set([
  408,
  425,
  429,
  500,
  502,
  503,
  504
]);

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    /*
     * ---------------------------------------------------------
     * Health check
     * ---------------------------------------------------------
     */
    if (request.method === 'GET' && url.pathname === '/') {
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

    /*
     * ---------------------------------------------------------
     * Slash command registration
     * ---------------------------------------------------------
     */
    if (request.method === 'GET' && url.pathname === '/register') {
      try {
        const result = await registerSlashCommands(
          env.DISCORD_BOT_TOKEN,
          env.DISCORD_APPLICATION_ID
        );

        return new Response(result.text, {
          status: result.status,
          headers: {
            'Content-Type': 'text/plain; charset=utf-8'
          }
        });
      } catch (err) {
        console.error('[REGISTER ERROR]', err);

        return new Response(
          `등록 실패: ${err?.message || String(err)}`,
          { status: 500 }
        );
      }
    }

    /*
     * ---------------------------------------------------------
     * Discord Interaction signature verification
     * ---------------------------------------------------------
     */
    if (request.method !== 'POST') {
      return new Response('Method Not Allowed', { status: 405 });
    }

    const signature = request.headers.get('X-Signature-Ed25519');
    const timestamp = request.headers.get('X-Signature-Timestamp');

    const body = await request.text();

    const valid = await verifyDiscordRequest(
      signature,
      timestamp,
      body,
      env.DISCORD_PUBLIC_KEY
    );

    if (!valid) {
      console.error('[SIGNATURE ERROR] Invalid Discord signature');

      return new Response(
        'Invalid request signature',
        { status: 401 }
      );
    }

    let interaction;

    try {
      interaction = JSON.parse(body);
    } catch {
      return new Response('Invalid JSON', { status: 400 });
    }

    /*
     * ---------------------------------------------------------
     * Discord PING
     * ---------------------------------------------------------
     */
    if (interaction.type === 1) {
      return json({
        type: 1
      });
    }

    /*
     * ---------------------------------------------------------
     * Slash Commands
     * ---------------------------------------------------------
     */
    if (interaction.type === 2) {
      return handleSlashCommand(interaction, env, ctx);
    }

    return new Response('Bad Request', { status: 400 });
  },

  /*
   * -----------------------------------------------------------
   * Cloudflare Queue Consumer
   * -----------------------------------------------------------
   */
  async queue(batch, env, ctx) {
    console.log(`[QUEUE] batch=${batch.messages.length}`);

    for (const message of batch.messages) {
      const jobId = message.body?.jobId;

      if (!jobId) {
        message.ack();
        continue;
      }

      console.log(`[QUEUE] job=${jobId}`);

      try {
        const result = await processCloneJob(
          jobId,
          env
        );

        /*
         * ACK
         *
         * processCloneJob()이 true를 반환하면
         * Worker 내부에서 실제 retry가 필요한 경우.
         */
        if (result === 'retry') {
          console.log(`[QUEUE RETRY] job=${jobId}`);
          message.retry();
        } else {
          message.ack();
        }
      } catch (err) {
        console.error(
          `[QUEUE EXCEPTION] job=${jobId}`,
          err?.stack || err
        );

        /*
         * 예외가 발생했을 때만 Queue retry.
         */
        message.retry();
      }
    }
  }
};


/* ============================================================
 * Slash Commands
 * ========================================================== */

async function handleSlashCommand(interaction, env, ctx) {
  const name = interaction.data?.name;
  const options = interaction.data?.options || [];

  const currentGuildId = interaction.guild_id;

  /*
   * /serverid
   */
  if (name === 'serverid') {
    return json({
      type: 4,
      data: {
        content:
          `📌 **현재 서버 ID:** \`${currentGuildId || 'DM'}\``
      }
    });
  }

  /*
   * /channelid
   */
  if (name === 'channelid') {
    const channelId = interaction.channel?.id;

    return json({
      type: 4,
      data: {
        content:
          `📌 **현재 채널 ID:** \`${channelId || '알 수 없음'}\``
      }
    });
  }

  /*
   * /backup
   */
  if (name === 'backup') {
    const guildOption = options.find(
      x => x.name === 'guild_id'
    );

    const guildId =
      guildOption?.value?.trim() ||
      currentGuildId;

    if (!guildId) {
      return json({
        type: 4,
        data: {
          content:
            '❌ 백업할 서버 ID를 확인할 수 없습니다.'
        }
      });
    }

    /*
     * 먼저 Discord에 즉시 응답.
     * 이후 waitUntil에서 실제 백업.
     */
    ctx.waitUntil(
      handleBackup(
        guildId,
        env,
        interaction.application_id || env.DISCORD_APPLICATION_ID,
        interaction.token
      )
    );

    return json({
      type: 5
    });
  }

  /*
   * /clone
   */
  if (name === 'clone') {
    const source =
      options.find(
        x => x.name === 'source_guild_id'
      )?.value?.trim();

    const target =
      options.find(
        x => x.name === 'target_guild_id'
      )?.value?.trim();

    if (!source || !target) {
      return json({
        type: 4,
        data: {
          content:
            '❌ 사용법: `/clone source_guild_id target_guild_id`'
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

      interactionToken: interaction.token,

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

      /*
       * 원본 Role ID -> 대상 Role ID
       */
      roleMap: {},

      /*
       * 원본 Category ID -> 대상 Category ID
       */
      categoryMap: {},

      /*
       * 원본 Channel ID -> 대상 Channel ID
       */
      channelMap: {},

      /*
       * 채널별 일시 오류 횟수
       */
      channelRetries: {},

      categoryRetries: {},

      roleRetries: {},

      /*
       * 이미 처리된 항목의 ID.
       */
      processedRoles: {},
      processedCategories: {},
      processedChannels: {},

      botHighestRolePosition: 0,
      targetEveryoneRoleId: target,

      /*
       * Progress PATCH 중복 방지
       */
      lastProgress: '',
      lastProgressAt: 0,

      lockedUntil: 0,

      createdAt: Date.now(),
      startedAt: Date.now(),

      lastError: null
    };

    await env.SERVER_BACKUPS.put(
      jobId,
      JSON.stringify(job)
    );

    console.log(
      `[CLONE CREATE] job=${jobId} source=${source} target=${target}`
    );

    /*
     * 최초 Queue 전송
     */
    if (!env.CLONE_QUEUE) {
      console.error('[QUEUE ERROR] CLONE_QUEUE binding missing');

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

    /*
     * defer response
     *
     * Discord에서 "로딩 중..." 상태가 너무 오래 지속되는
     * 문제를 줄이기 위해 즉시 deferred response.
     */
    return json({
      type: 5
    });
  }

  return json({
    type: 4,
    data: {
      content: '❌ 알 수 없는 명령어입니다.'
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
  const startedAt = Date.now();

  try {
    await updateProgress(
      applicationId,
      interactionToken,
      `📦 **서버 백업을 시작합니다...**\n\n` +
      `🎯 서버: \`${guildId}\`\n` +
      `⏳ 서버 정보를 가져오는 중...`
    );

    /*
     * Roles
     */
    const rolesRes = await discordApi(
      `/guilds/${guildId}/roles`,
      env.DISCORD_BOT_TOKEN,
      {},
      '백업 역할 조회'
    );

    if (!rolesRes.ok) {
      throw new Error(
        `역할 조회 실패 (${rolesRes.status})`
      );
    }

    /*
     * Channels
     */
    const channelsRes = await discordApi(
      `/guilds/${guildId}/channels`,
      env.DISCORD_BOT_TOKEN,
      {},
      '백업 채널 조회'
    );

    if (!channelsRes.ok) {
      throw new Error(
        `채널 조회 실패 (${channelsRes.status})`
      );
    }

    const roles =
      Array.isArray(rolesRes.data)
        ? rolesRes.data
        : [];

    const channels =
      Array.isArray(channelsRes.data)
        ? channelsRes.data
        : [];

    /*
     * 필요한 데이터만 저장.
     */
    const backupData = {
      guildId,

      roles: roles.map(role => ({
        id: role.id,
        name: role.name,
        permissions: String(role.permissions || '0'),
        color: role.color || 0,
        hoist: !!role.hoist,
        mentionable: !!role.mentionable,
        managed: !!role.managed,
        position: role.position || 0
      })),

      channels: channels.map(channel => ({
        id: channel.id,
        name: channel.name,
        type: Number(channel.type),
        position: channel.position || 0,
        parent_id: channel.parent_id || null,

        topic:
          channel.topic !== undefined
            ? channel.topic
            : null,

        bitrate:
          channel.bitrate !== undefined
            ? channel.bitrate
            : null,

        user_limit:
          channel.user_limit !== undefined
            ? channel.user_limit
            : null,

        rate_limit_per_user:
          channel.rate_limit_per_user !== undefined
            ? channel.rate_limit_per_user
            : null,

        nsfw:
          channel.nsfw !== undefined
            ? channel.nsfw
            : null,

        permission_overwrites:
          Array.isArray(channel.permission_overwrites)
            ? channel.permission_overwrites
            : []
      }))
    };

    await env.SERVER_BACKUPS.put(
      `backup_${guildId}`,
      JSON.stringify(backupData)
    );

    const elapsed =
      ((Date.now() - startedAt) / 1000).toFixed(1);

    const content =
      `✅ 서버(\`${guildId}\`) 백업이 완료되었습니다!\n\n` +
      `👤 역할: ${backupData.roles.length}\n` +
      `💬 채널: ${backupData.channels.length}\n` +
      `⏱️ 소요시간: ${elapsed}초`;

    await updateProgress(
      applicationId,
      interactionToken,
      content,
      true
    );

    console.log(
      `[BACKUP COMPLETE] guild=${guildId} roles=${backupData.roles.length} channels=${backupData.channels.length} time=${elapsed}s`
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
      `사유: \`${escapeDiscordText(err?.message || String(err))}\``,
      true
    );
  }
}


/* ============================================================
 * Clone Job
 * ========================================================== */

async function processCloneJob(jobId, env) {
  const raw = await env.SERVER_BACKUPS.get(jobId);

  if (!raw) {
    console.log(`[JOB SKIP] job=${jobId} data-not-found`);
    return 'ack';
  }

  let job;

  try {
    job = JSON.parse(raw);
  } catch {
    console.error(`[JOB ERROR] job=${jobId} invalid-json`);
    return 'ack';
  }

  if (job.status !== 'running') {
    console.log(
      `[JOB SKIP] job=${jobId} status=${job.status}`
    );

    return 'ack';
  }

  /*
   * ---------------------------------------------------------
   * LOCK
   * ---------------------------------------------------------
   *
   * 중요한 부분:
   *
   * 기존 코드:
   * locked -> return true -> retry()
   *
   * 이 구조가 Queue 폭발의 원인이었음.
   *
   * 이제 locked이면 그냥 ACK.
   */
  const now = Date.now();

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

  await saveJob(env, job);

  const progress = createProgressUpdater(
    env,
    job
  );

  try {
    /*
     * Backup data
     */
    const backupRaw =
      await env.SERVER_BACKUPS.get(
        `backup_${job.sourceGuildId}`
      );

    if (!backupRaw) {
      throw permanentError(
        '원본 서버의 백업 데이터가 없습니다. 먼저 /backup을 실행해주세요.'
      );
    }

    const backup = JSON.parse(backupRaw);

    const roles =
      Array.isArray(backup.roles)
        ? [...backup.roles]
        : [];

    const channels =
      Array.isArray(backup.channels)
        ? [...backup.channels]
        : [];

    /*
     * Categories
     */
    const categories =
      channels
        .filter(
          x => Number(x.type) === 4
        )
        .sort(
          (a, b) =>
            (a.position || 0) -
            (b.position || 0)
        );

    /*
     * Normal channels
     */
    const normalChannels =
      channels
        .filter(
          x => Number(x.type) !== 4
        )
        .sort(
          (a, b) =>
            (a.position || 0) -
            (b.position || 0)
        );

    /*
     * ---------------------------------------------------------
     * INIT
     * ---------------------------------------------------------
     */

    if (job.phase === 'INIT') {
      const setup = await prepareTargetServer(
        job,
        env
      );

      job.targetEveryoneRoleId =
        setup.everyoneRoleId;

      job.botHighestRolePosition =
        setup.botHighestRolePosition;

      job.phase = 'ROLES';

      await saveJob(env, job);

      await progress(
        buildProgress(job, roles, categories, normalChannels)
      );
    }

    /*
     * ---------------------------------------------------------
     * ROLES
     * ---------------------------------------------------------
     */

    if (job.phase === 'ROLES') {
      const result =
        await processRoles(
          job,
          roles,
          env
        );

      await saveJob(env, job);

      if (result === 'retry') {
        return 'retry';
      }

      if (job.roleIndex < roles.length) {
        await progress(
          buildProgress(
            job,
            roles,
            categories,
            normalChannels
          )
        );

        await enqueueNext(env, job);

        job.lockedUntil = 0;
        await saveJob(env, job);

        return 'ack';
      }

      /*
       * Role hierarchy
       */
      job.phase = 'ROLES_SYNC';

      await saveJob(env, job);
    }

    /*
     * ---------------------------------------------------------
     * ROLE HIERARCHY
     * ---------------------------------------------------------
     */

    if (job.phase === 'ROLES_SYNC') {
      await synchronizeRoleHierarchy(
        job,
        env
      );

      job.phase = 'CATEGORIES';

      await saveJob(env, job);
    }

    /*
     * ---------------------------------------------------------
     * CATEGORIES
     * ---------------------------------------------------------
     */

    if (job.phase === 'CATEGORIES') {
      const result =
        await processCategories(
          job,
          categories,
          env
        );

      await saveJob(env, job);

      if (result === 'retry') {
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

        await enqueueNext(env, job);

        job.lockedUntil = 0;
        await saveJob(env, job);

        return 'ack';
      }

      job.phase = 'CHANNELS';

      await saveJob(env, job);
    }

    /*
     * ---------------------------------------------------------
     * CHANNELS
     * ---------------------------------------------------------
     */

    if (job.phase === 'CHANNELS') {
      const result =
        await processChannels(
          job,
          normalChannels,
          env
        );

      await saveJob(env, job);

      if (result === 'retry') {
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

        await enqueueNext(env, job);

        job.lockedUntil = 0;
        await saveJob(env, job);

        return 'ack';
      }

      job.phase = 'CHANNEL_SYNC';

      await saveJob(env, job);
    }

    /*
     * ---------------------------------------------------------
     * CHANNEL POSITION SYNC
     * ---------------------------------------------------------
     */

    if (job.phase === 'CHANNEL_SYNC') {
      await synchronizeChannelPositions(
        job,
        normalChannels,
        env
      );

      job.phase = 'COMPLETED';
      job.status = 'completed';

      await saveJob(env, job);
    }

    /*
     * ---------------------------------------------------------
     * COMPLETE
     * ---------------------------------------------------------
     */

    if (job.phase === 'COMPLETED') {
      const elapsed =
        ((Date.now() - job.startedAt) / 1000)
          .toFixed(1);

      const totalFailed =
        job.roleFailed +
        job.categoryFailed +
        job.channelFailed;

      let content;

      if (totalFailed === 0) {
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
          `👤 역할: ${job.roleSuccess}/${roles.length}` +
          ` (실패 ${job.roleFailed})\n` +
          `📁 카테고리: ${job.categorySuccess}/${categories.length}` +
          ` (실패 ${job.categoryFailed})\n` +
          `💬 채널: ${job.channelSuccess}/${normalChannels.length}` +
          ` (실패 ${job.channelFailed})\n` +
          `⏱️ 소요 시간: ${elapsed}초`;
      }

      await progress(
        content,
        true
      );

      console.log(
        `[JOB COMPLETE] ${jobId}`
      );

      /*
       * Job 데이터는 잠시 남겨둔다.
       * 바로 삭제하지 않아야 디버깅 가능.
       */
      job.lockedUntil = 0;

      await saveJob(env, job);

      return 'ack';
    }

    job.lockedUntil = 0;
    await saveJob(env, job);

    return 'ack';

  } catch (err) {
    console.error(
      `[JOB ERROR] job=${jobId}`,
      err?.stack || err
    );

    /*
     * Permanent error
     */
    if (err?.permanent) {
      job.status = 'failed';
      job.lastError =
        err?.message ||
        String(err);

      job.failedAt = Date.now();
      job.lockedUntil = 0;

      await saveJob(env, job);

      await progress(
        `❌ **서버 복제에 실패했습니다.**\n\n` +
        `사유: \`${escapeDiscordText(job.lastError)}\``,
        true
      );

      return 'ack';
    }

    /*
     * Temporary error
     */
    job.lockedUntil = 0;

    await saveJob(env, job);

    return 'retry';
  }
}


/* ============================================================
 * Prepare Target Server
 * ========================================================== */

async function prepareTargetServer(job, env) {
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
    Array.isArray(rolesRes.data)
      ? rolesRes.data
      : [];

  const everyone =
    roles.find(
      x => x.name === '@everyone'
    );

  const everyoneRoleId =
    everyone?.id ||
    job.targetGuildId;

  /*
   * Bot user
   */
  const me =
    await discordApi(
      `/users/@me`,
      token,
      {},
      '봇 정보'
    );

  let botHighestRolePosition = 0;

  if (me.ok && me.data?.id) {
    const member =
      await discordApi(
        `/guilds/${job.targetGuildId}/members/${me.data.id}`,
        token,
        {},
        '봇 멤버 정보'
      );

    if (member.ok) {
      const botRoleIds =
        Array.isArray(member.data?.roles)
          ? member.data.roles
          : [];

      for (const role of roles) {
        if (
          role.id !== everyoneRoleId &&
          botRoleIds.includes(role.id)
        ) {
          botHighestRolePosition =
            Math.max(
              botHighestRolePosition,
              Number(role.position || 0)
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

async function processRoles(job, roles, env) {
  const token =
    env.DISCORD_BOT_TOKEN;

  roles.sort(
    (a, b) =>
      (a.position || 0) -
      (b.position || 0)
  );

  const end =
    Math.min(
      job.roleIndex +
      MAX_ROLES_PER_RUN,
      roles.length
    );

  while (job.roleIndex < end) {
    const role =
      roles[job.roleIndex];

    /*
     * @everyone
     */
    if (role.name === '@everyone') {
      job.roleMap[role.id] =
        job.targetEveryoneRoleId;

      job.roleSuccess++;

      job.processedRoles[role.id] = true;

      job.roleIndex++;

      continue;
    }

    /*
     * Managed roles cannot be recreated.
     */
    if (role.managed) {
      job.processedRoles[role.id] = true;
      job.roleIndex++;
      continue;
    }

    /*
     * Already processed.
     */
    if (job.processedRoles[role.id]) {
      job.roleIndex++;
      continue;
    }

    const res =
      await discordApi(
        `/guilds/${job.targetGuildId}/roles`,
        token,
        {
          method: 'POST',

          headers: {
            'Content-Type': 'application/json'
          },

          body: JSON.stringify({
            name: role.name,
            permissions:
              String(role.permissions || '0'),
            color:
              Number(role.color || 0),
            hoist:
              !!role.hoist,
            mentionable:
              !!role.mentionable
          })
        },
        `역할 생성 "${role.name}"`
      );

    if (res.ok && res.data?.id) {
      job.roleSuccess++;

      job.roleMap[role.id] =
        res.data.id;

      job.processedRoles[role.id] = true;

      job.roleIndex++;

      continue;
    }

    /*
     * Discord가 일시 오류를 반환하면
     * Queue retry.
     */
    if (TEMPORARY_ERRORS.has(res.status)) {
      return 'retry';
    }

    /*
     * 권한 등 영구 실패.
     */
    job.roleFailed++;
    job.processedRoles[role.id] = true;
    job.roleIndex++;
  }

  return 'ok';
}


/* ============================================================
 * Role Hierarchy
 * ========================================================== */

async function synchronizeRoleHierarchy(job, env) {
  if (!job.roleMap) return;

  const payload = [];

  /*
   * 원본 role position 기준으로 정렬.
   */
  const entries =
    Object.entries(job.roleMap);

  /*
   * 현재 대상 역할 조회
   */
  const rolesRes =
    await discordApi(
      `/guilds/${job.targetGuildId}/roles`,
      env.DISCORD_BOT_TOKEN,
      {},
      '역할 계층 확인'
    );

  if (!rolesRes.ok) {
    console.log(
      `[HIERARCHY] 조회 실패 status=${rolesRes.status}`
    );

    return;
  }

  const targetRoles =
    Array.isArray(rolesRes.data)
      ? rolesRes.data
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

  /*
   * bot position은 job에 저장된 값을 사용.
   */
  for (const [sourceId, targetId] of entries) {
    if (
      sourceId === job.sourceGuildId
    ) {
      continue;
    }

    const targetRole =
      targetMap.get(targetId);

    if (!targetRole) {
      continue;
    }

    if (
      targetRole.position >
      job.botHighestRolePosition
    ) {
      console.log(
        `[HIERARCHY SKIP] role=${targetRole.name} sourcePosition=${targetRole.position} botPosition=${job.botHighestRolePosition}`
      );

      continue;
    }

    /*
     * 실제 PATCH는 아래에서 한 번에 처리.
     */
  }

  /*
   * 원본 backup에서 role position을 찾기 위해
   * job에 저장된 rolePositionMap이 없으면 종료.
   *
   * 실제 순서는 Discord 생성 순서가 대부분 유지되므로
   * 권한 범위를 벗어나지 않는 경우에만 최소한 동기화.
   */
  if (!job.rolePositionMap) {
    return;
  }

  for (const [sourceId, targetId] of entries) {
    const sourcePosition =
      Number(
        job.rolePositionMap[sourceId] ??
        0
      );

    const targetRole =
      targetMap.get(targetId);

    if (!targetRole) {
      continue;
    }

    if (
      sourcePosition >
      job.botHighestRolePosition
    ) {
      continue;
    }

    payload.push({
      id: targetId,
      position: sourcePosition
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
          'Content-Type': 'application/json'
        },

        body: JSON.stringify(payload)
      },
      '역할 순서 동기화'
    );

  /*
   * Missing Permissions는
   * 복제 전체 실패로 만들지 않는다.
   */
  if (
    !res.ok &&
    res.status === 403
  ) {
    console.log(
      '[HIERARCHY] 권한 부족으로 역할 순서 일부를 건너뜁니다.'
    );

    return;
  }

  if (
    !res.ok &&
    res.status === 400 &&
    res.data?.code === 50013
  ) {
    console.log(
      '[HIERARCHY] Discord Missing Permissions - skip'
    );

    return;
  }

  if (
    !res.ok &&
    TEMPORARY_ERRORS.has(res.status)
  ) {
    throw new Error(
      `역할 순서 동기화 일시 오류 ${res.status}`
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

  const end =
    Math.min(
      job.categoryIndex +
      MAX_CATEGORIES_PER_RUN,
      categories.length
    );

  while (
    job.categoryIndex < end
  ) {
    const cat =
      categories[job.categoryIndex];

    /*
     * 이미 처리됨
     */
    if (
      job.processedCategories[cat.id]
    ) {
      job.categoryIndex++;
      continue;
    }

    const permissionOverwrites =
      mapPermissionOverwrites(
        cat.permission_overwrites,
        job
      );

    const body = {
      name: cat.name,
      type: 4,
      permission_overwrites:
        permissionOverwrites
    };

    /*
     * 같은 이름의 카테고리가 이미 있다면
     * 중복 생성을 방지하기 위해 기존 것을 사용.
     */
    const existing =
      await findExistingChannel(
        job.targetGuildId,
        cat,
        null,
        env
      );

    if (existing) {
      job.categoryMap[cat.id] =
        existing.id;

      job.processedCategories[cat.id] =
        true;

      job.categorySuccess++;
      job.categoryIndex++;

      continue;
    }

    const res =
      await discordApi(
        `/guilds/${job.targetGuildId}/channels`,
        token,
        {
          method: 'POST',

          headers: {
            'Content-Type': 'application/json'
          },

          body: JSON.stringify(body)
        },
        `카테고리 생성 "${cat.name}"`
      );

    if (
      res.ok &&
      res.data?.id
    ) {
      job.categoryMap[cat.id] =
        res.data.id;

      job.processedCategories[cat.id] =
        true;

      job.categorySuccess++;

      job.categoryIndex++;

      continue;
    }

    if (
      TEMPORARY_ERRORS.has(res.status)
    ) {
      return 'retry';
    }

    job.categoryFailed++;
    job.processedCategories[cat.id] =
      true;

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

  const end =
    Math.min(
      job.channelIndex +
      MAX_CHANNELS_PER_RUN,
      channels.length
    );

  while (
    job.channelIndex < end
  ) {
    const channel =
      channels[job.channelIndex];

    /*
     * 이미 성공 처리된 경우.
     */
    if (
      job.processedChannels[channel.id]
    ) {
      job.channelIndex++;
      continue;
    }

    /*
     * Category mapping.
     */
    let parentId = null;

    if (
      channel.parent_id &&
      job.categoryMap[channel.parent_id]
    ) {
      parentId =
        job.categoryMap[channel.parent_id];
    }

    /*
     * Discord channel type.
     *
     * type 5 = announcement
     *
     * 대상 서버에서 Announcement Channel을
     * 사용할 수 없는 경우 text channel로 fallback.
     */
    const originalType =
      Number(channel.type);

    let mappedType =
      originalType;

    const allowedTypes = [
      0, 2, 4,
      5,
      6,
      13,
      14,
      15,
      16,
      21
    ];

    if (
      !allowedTypes.includes(mappedType)
    ) {
      mappedType = 0;
    }

    /*
     * Permission overwrites
     */
    const permissionOverwrites =
      mapPermissionOverwrites(
        channel.permission_overwrites,
        job
      );

    const body = {
      name: channel.name,
      type: mappedType,
      permission_overwrites:
        permissionOverwrites
    };

    if (parentId) {
      body.parent_id = parentId;
    }

    /*
     * Text
     */
    if (mappedType === 0) {
      if (channel.topic != null) {
        body.topic =
          channel.topic;
      }

      if (channel.nsfw != null) {
        body.nsfw =
          !!channel.nsfw;
      }

      if (
        channel.rate_limit_per_user != null
      ) {
        body.rate_limit_per_user =
          Number(
            channel.rate_limit_per_user
          );
      }
    }

    /*
     * Announcement
     */
    if (mappedType === 5) {
      if (channel.topic != null) {
        body.topic =
          channel.topic;
      }
    }

    /*
     * Voice / Stage
     */
    if (
      mappedType === 2 ||
      mappedType === 13
    ) {
      if (channel.bitrate != null) {
        body.bitrate =
          Number(channel.bitrate);
      }

      if (
        channel.user_limit != null
      ) {
        body.user_limit =
          Number(channel.user_limit);
      }
    }

    /*
     * ---------------------------------------------------------
     * 중복 채널 검사
     * ---------------------------------------------------------
     */
    const existing =
      await findExistingChannel(
        job.targetGuildId,
        channel,
        parentId,
        env
      );

    if (existing) {
      job.channelMap[channel.id] =
        existing.id;

      job.processedChannels[channel.id] =
        true;

      job.channelSuccess++;

      job.channelIndex++;

      continue;
    }

    /*
     * ---------------------------------------------------------
     * Create
     * ---------------------------------------------------------
     */
    const res =
      await discordApi(
        `/guilds/${job.targetGuildId}/channels`,
        token,
        {
          method: 'POST',

          headers: {
            'Content-Type': 'application/json'
          },

          body: JSON.stringify(body)
        },
        `채널 생성 "${channel.name}"`
      );

    if (
      res.ok &&
      res.data?.id
    ) {
      job.channelMap[channel.id] =
        res.data.id;

      job.processedChannels[channel.id] =
        true;

      job.channelSuccess++;

      job.channelIndex++;

      continue;
    }

    /*
     * 일시 오류
     *
     * 같은 채널에서 계속 실패하더라도
     * Queue가 전체 job을 무한히 반복하지 않도록
     * 몇 번까지만 retry.
     */
    if (
      TEMPORARY_ERRORS.has(res.status)
    ) {
      const count =
        Number(
          job.channelRetries[channel.id] ||
          0
        );

      if (count < 3) {
        job.channelRetries[channel.id] =
          count + 1;

        await saveJob(env, job);

        console.log(
          `[CHANNEL RETRY] name=${channel.name} attempt=${count + 1}`
        );

        return 'retry';
      }

      /*
       * 3번 실패하면 해당 채널만 실패 처리하고
       * 다음 채널로 진행.
       */
      console.log(
        `[CHANNEL FAILED] name=${channel.name} retries-exhausted`
      );

      job.channelFailed++;

      job.processedChannels[channel.id] =
        true;

      job.channelIndex++;

      continue;
    }

    /*
     * 403 / 400 등의 영구 오류.
     */
    console.log(
      `[CHANNEL SKIP] name=${channel.name} status=${res.status}`
    );

    job.channelFailed++;

    job.processedChannels[channel.id] =
      true;

    job.channelIndex++;
  }

  return 'ok';
}


/* ============================================================
 * Existing Channel Detection
 * ========================================================== */

async function findExistingChannel(
  guildId,
  sourceChannel,
  targetParentId,
  env
) {
  const res =
    await discordApi(
      `/guilds/${guildId}/channels`,
      env.DISCORD_BOT_TOKEN,
      {},
      '중복 채널 확인'
    );

  if (!res.ok) {
    /*
     * 중복 확인 자체가 실패하면
     * 생성 로직으로 넘어간다.
     */
    return null;
  }

  const channels =
    Array.isArray(res.data)
      ? res.data
      : [];

  const sourceType =
    Number(sourceChannel.type);

  /*
   * type 5 announcement는
   * 대상에서 0으로 fallback 될 수 있으므로
   * 둘 다 비교.
   */
  const candidateTypes =
    sourceType === 5
      ? [5, 0]
      : [sourceType];

  return (
    channels.find(channel => {
      if (
        channel.name !==
        sourceChannel.name
      ) {
        return false;
      }

      if (
        !candidateTypes.includes(
          Number(channel.type)
        )
      ) {
        return false;
      }

      const parent =
        channel.parent_id || null;

      return (
        parent ===
        (targetParentId || null)
      );
    }) ||
    null
  );
}


/* ============================================================
 * Permission Overwrite Mapping
 * ========================================================== */

function mapPermissionOverwrites(
  overwrites,
  job
) {
  if (!Array.isArray(overwrites)) {
    return [];
  }

  return overwrites
    .filter(
      ow => Number(ow.type) === 0
    )
    .map(ow => {
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
       * Normal role
       */
      else if (
        job.roleMap &&
        job.roleMap[ow.id]
      ) {
        mappedId =
          job.roleMap[ow.id];
      }

      if (!mappedId) {
        return null;
      }

      return {
        id: mappedId,

        type: 0,

        allow:
          String(ow.allow || '0'),

        deny:
          String(ow.deny || '0')
      };
    })
    .filter(Boolean);
}


/* ============================================================
 * Channel Position Sync
 * ========================================================== */

async function synchronizeChannelPositions(
  job,
  channels,
  env
) {
  /*
   * 채널 생성 API의 position을 처음부터 강제로 지정하지 않고
   * 모든 채널 생성이 끝난 후 한 번에 정렬한다.
   *
   * 이 방식이 Discord의 생성 시점 position 충돌을
   * 크게 줄인다.
   */

  const payload = [];

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
    return;
  }

  /*
   * Discord의 position PATCH.
   *
   * 실패하더라도 이미 생성된 서버 구조 자체는
   * 정상적으로 남아 있으므로 전체 복제를 실패시키지 않는다.
   */
  const res =
    await discordApi(
      `/guilds/${job.targetGuildId}/channels`,
      env.DISCORD_BOT_TOKEN,
      {
        method: 'PATCH',

        headers: {
          'Content-Type': 'application/json'
        },

        body: JSON.stringify(payload)
      },
      '채널 순서 동기화'
    );

  if (!res.ok) {
    console.log(
      `[CHANNEL POSITION SKIP] status=${res.status}`
    );
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
      content === job.lastProgress
    ) {
      return;
    }

    if (
      !force &&
      now - job.lastProgressAt <
      PROGRESS_MIN_INTERVAL_MS
    ) {
      return;
    }

    job.lastProgress =
      content;

    job.lastProgressAt =
      now;

    await saveJob(env, job);

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
      await fetch(url, {
        method: 'PATCH',

        headers: {
          'Content-Type':
            'application/json'
        },

        body: JSON.stringify({
          content,

          allowed_mentions: {
            parse: []
          }
        })
      });

    if (!res.ok) {
      const text =
        await res.text();

      console.error(
        `[PROGRESS ERROR] status=${res.status} response=${text.slice(0, 300)}`
      );

      /*
       * Interaction token이 만료됐거나
       * original message가 없는 경우에는
       * 무한 재시도하지 않는다.
       */
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
 * Progress Message Builder
 * ========================================================== */

function buildProgress(
  job,
  roles,
  categories,
  channels
) {
  const roleTotal =
    roles.length;

  const categoryTotal =
    categories.length;

  const channelTotal =
    channels.length;

  let phaseText =
    '⏳ 처리 중...';

  if (job.phase === 'ROLES') {
    phaseText =
      '👤 역할을 생성하는 중...';
  }

  if (job.phase === 'ROLES_SYNC') {
    phaseText =
      '🔃 역할 순서를 동기화하는 중...';
  }

  if (job.phase === 'CATEGORIES') {
    phaseText =
      '📁 카테고리를 생성하는 중...';
  }

  if (job.phase === 'CHANNELS') {
    phaseText =
      '💬 채널을 생성하는 중...';
  }

  return (
    `🚀 **서버 복제 진행 중**\n\n` +

    `👤 역할: ` +
    `${job.roleSuccess}/${roleTotal}\n` +

    `🔃 역할 순서: ` +
    `${job.phase === 'ROLES_SYNC' || job.phase === 'CATEGORIES' || job.phase === 'CHANNELS' ? '처리 완료' : '처리 중'}\n` +

    `📁 카테고리: ` +
    `${job.categorySuccess}/${categoryTotal}\n` +

    `💬 채널: ` +
    `${job.channelSuccess}/${channelTotal}\n\n` +

    `${phaseText}`
  );
}


/* ============================================================
 * Queue
 * ========================================================== */

async function enqueueNext(env, job) {
  if (!env.CLONE_QUEUE) {
    throw new Error(
      'CLONE_QUEUE binding missing'
    );
  }

  console.log(
    `[QUEUE NEXT] job=${job.jobId} phase=${job.phase}`
  );

  await env.CLONE_QUEUE.send({
    jobId: job.jobId
  });
}


/* ============================================================
 * Job Storage
 * ========================================================== */

async function saveJob(env, job) {
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
        await fetch(url, {
          ...options,

          headers: {
            Authorization:
              `Bot ${token}`,

            ...(options.headers || {})
          }
        });

      const text =
        await res.text();

      let data = null;

      if (text) {
        try {
          data =
            JSON.parse(text);
        } catch {
          data = null;
        }
      }

      /*
       * 429
       */
      if (res.status === 429) {
        const retryAfter =
          Number(
            data?.retry_after ||
            res.headers.get(
              'Retry-After'
            ) ||
            1
          );

        console.log(
          `[429] ${label} wait=${retryAfter}s`
        );

        /*
         * 너무 긴 sleep 방지.
         * 긴 rate limit은 Queue retry.
         */
        if (
          retryAfter > 5
        ) {
          return {
            ok: false,
            status: 429,
            data,
            text
          };
        }

        await sleep(
          retryAfter * 1000
        );

        continue;
      }

      /*
       * Temporary server error.
       */
      if (
        TEMPORARY_ERRORS.has(
          res.status
        )
      ) {
        console.log(
          `[DISCORD TEMP] ${label} status=${res.status} attempt=${attempt}`
        );

        if (
          attempt <
          maxAttempts
        ) {
          await sleep(
            attempt * 1000
          );

          continue;
        }
      }

      /*
       * Normal error.
       */
      if (!res.ok) {
        console.error(
          `[DISCORD ERROR] ${label} status=${res.status} code=${data?.code || '-'}`
        );
      } else {
        console.log(
          `[DISCORD OK] ${label} ${res.status}`
        );
      }

      return {
        ok: res.ok,
        status: res.status,
        statusText:
          res.statusText,
        data,
        text
      };

    } catch (err) {
      console.error(
        `[DISCORD FETCH] ${label} attempt=${attempt}`,
        err?.message || err
      );

      if (
        attempt <
        maxAttempts
      ) {
        await sleep(
          attempt * 1000
        );

        continue;
      }

      return {
        ok: false,
        status: 0,
        data: null,
        text:
          err?.message ||
          String(err)
      };
    }
  }

  return {
    ok: false,
    status: 503,
    data: null,
    text: 'Maximum attempts reached'
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
        const bytes =
          new Uint8Array(
            hex.length / 2
          );

        for (
          let i = 0;
          i < bytes.length;
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
      hexToBytes(publicKey);

    const signatureBytes =
      hexToBytes(signature);

    const messageBytes =
      new TextEncoder().encode(
        timestamp + body
      );

    /*
     * Cloudflare Workers
     */
    const key =
      await crypto.subtle.importKey(
        'raw',
        publicKeyBytes,
        {
          name: 'Ed25519'
        },
        false,
        ['verify']
      );

    return await crypto.subtle.verify(
      {
        name: 'Ed25519'
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
 * Slash Command Registration
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
      name: 'serverid',
      description:
        '현재 서버의 ID를 확인합니다.'
    },

    {
      name: 'channelid',
      description:
        '현재 채널의 ID를 확인합니다.'
    },

    {
      name: 'backup',
      description:
        '서버의 역할과 채널 구조를 백업합니다.',

      options: [
        {
          type: 3,
          name: 'guild_id',
          description:
            '백업할 서버 ID',
          required: false
        }
      ]
    },

    {
      name: 'clone',
      description:
        '백업된 서버 구조를 다른 서버에 복제합니다.',

      options: [
        {
          type: 3,
          name: 'source_guild_id',
          description:
            '원본 서버 ID',
          required: true
        },

        {
          type: 3,
          name: 'target_guild_id',
          description:
            '대상 서버 ID',
          required: true
        }
      ]
    }
  ];

  const url =
    `${DISCORD_API}/applications/` +
    `${applicationId}/commands`;

  const res =
    await fetch(url, {
      method: 'PUT',

      headers: {
        Authorization:
          `Bot ${token}`,

        'Content-Type':
          'application/json'
      },

      body:
        JSON.stringify(commands)
    });

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
    status: res.status,
    text:
      `명령어 등록 실패:\n${text}`
  };
}


/* ============================================================
 * Utilities
 * ========================================================== */

function json(data, status = 200) {
  return new Response(
    JSON.stringify(data),
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


function sleep(ms) {
  return new Promise(
    resolve =>
      setTimeout(resolve, ms)
  );
}


function permanentError(message) {
  /** @type {Error & { permanent?: boolean }} */
  const err =
    new Error(message);

  err.permanent = true;

  return err;
}


function escapeDiscordText(text) {
  return String(text)
    .replace(/\\/g, '\\\\')
    .replace(/`/g, '\\`');
}
