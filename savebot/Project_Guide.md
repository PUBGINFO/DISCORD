# Discord Server Cloner Bot (Cloudflare Worker)

채팅을 제외한 **디스코드 서버의 카테고리, 채널, 역할 구조**를 백업하고 복원/복제하는 서버리스 봇입니다. Cloudflare Worker와 GitHub 연동을 통해 무료로 간편하게 호스팅할 수 있습니다.

## 🚀 설치 및 배포 방법

1. **GitHub 저장소 생성**
   - 위 파일들(`worker.js`, `wrangler.toml`, `README.md`)을 새 GitHub 레포지토리에 업로드합니다.

2. **Cloudflare Worker 설정**
   - Cloudflare 대시보드 -> **Workers & Pages** -> **Create application** -> **Connect to Git**을 선택하여 저장소를 연동합니다.

3. **Cloudflare KV 생성**
   - **Workers KV**로 이동하여 네임스페이스를 생성하고(`SERVER_BACKUPS`), `wrangler.toml`의 `id` 값에 입력합니다.

4. **환경 변수 설정 (Cloudflare 대시보드 Settings > Variables)**
   - `DISCORD_BOT_TOKEN`: 디스코드 봇 토큰
   - `DISCORD_PUBLIC_KEY`: 디스코드 앱의 Public Key

5. **디스코드 봇 설정**
   - Discord Developer Portal에서 **Interactions Endpoint URL**에 Cloudflare Worker 배포 URL을 입력합니다.
   - 봇에게 `Manage Roles`, `Manage Channels`, `Administrator` 권한을 부여하여 서버에 초대합니다.
   - 슬래시 커맨드 `/backup`, `/clone`을 사용합니다.