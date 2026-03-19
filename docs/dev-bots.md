# AgentDesk Dev 봇 목록

개발망 서버 (guild: `541999748245225509`) 전용 봇.
Release(윤호네)와 토큰이 완전 분리되어 있어 동시 운영 가능.

## 봇 4종

| 역할 | 봇 이름 | Bot ID | 용도 |
|------|---------|--------|------|
| Announce | AgentDesk-Announce | `1483997472736022598` | CEO 메시지 전달, `!` 텍스트 커맨드 트리거, `/api/send` 라우팅 |
| Notify | AgentDesk-Notify | `1483997249284473053` | 알림 발송, 채널 생성/삭제/수정 (관리자 권한) |
| Claude | AgentDesk-Claude | `1483997962194653185` | Claude 에이전트 세션 (CC 채널) |
| Codex | AgentDesk-Codex | `1483997680966434836` | Codex 에이전트 세션 (CDX 채널) |

## 설정 파일 위치

- **agentdesk.yaml**: `~/.adk/dev/agentdesk.yaml` — 4종 봇 토큰 + bot_id
- **bot_settings.json**: `~/.adk/dev/config/bot_settings.json` — Claude/Codex 세션 설정
- **role_map.json**: `~/.adk/dev/config/role_map.json` — 채널 ↔ 에이전트 매핑

## Release 봇 (윤호네 서버, RCC 운영 중)

| 역할 | Bot ID | 출처 |
|------|--------|------|
| Announce | `1479017284805722200` | bot_settings.json allowed_bot_ids |
| Claude | `1474932782395293736` | bot_settings.json 토큰 디코딩 |
| Codex | `1479425196824989758` | bot_settings.json 토큰 디코딩 |
| Notify | 미확인 | RCC에 별도 기록 없음 — health HTTP 서버가 대신 수행 |

## 초대 링크 생성 공식

```
https://discord.com/oauth2/authorize?client_id={BOT_ID}&permissions={PERMS}&scope=bot%20applications.commands
```

- Claude/Codex: `permissions=2147609664`
- Announce: `permissions=8` (관리자)
- Notify: `permissions=268561488` (채널 관리 포함)
