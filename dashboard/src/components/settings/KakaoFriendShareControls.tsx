import { Link2, Loader2, MessageSquareShare, Send, Unlink } from "lucide-react";
import { useMemo, useRef, useState, type CSSProperties } from "react";
import * as api from "../../api";
import type {
  KakaoFriendView,
  KakaoFriendsPage,
  KakaoAccountSummary,
  KakaoSendResult,
  OperatorConnectorStatus,
} from "../../api";

interface KakaoFriendShareControlsProps {
  connector: OperatorConnectorStatus;
  onReload: () => void;
  secondaryActionClass: string;
  secondaryActionStyle: CSSProperties;
  tr: (ko: string, en: string) => string;
}

export interface PendingSendIntent {
  idempotencyKey: string;
  fingerprint: string;
}

export function kakaoSendIntentFingerprint(accountId: string, receiverUuids: Iterable<string>, text: string): string {
  return JSON.stringify({ account_id: accountId, receiver_uuids: [...receiverUuids].sort(), text });
}

export function kakaoMemoIntentFingerprint(accountId: string, text: string): string {
  return JSON.stringify({ account_id: accountId, target: "self", text });
}

export function isAllowedKakaoAuthorizeUrl(target: URL): boolean {
  return target.protocol === "https:"
    && target.hostname === "kauth.kakao.com"
    && target.port === ""
    && target.username === ""
    && target.password === ""
    && target.pathname === "/oauth/authorize";
}

export function resolveKakaoSendIntent(
  pendingIntent: PendingSendIntent | null,
  fingerprint: string,
  createIdempotencyKey: () => string,
): { intent: PendingSendIntent; replaysExisting: boolean } {
  if (pendingIntent?.fingerprint === fingerprint) {
    return { intent: pendingIntent, replaysExisting: true };
  }
  return {
    intent: { idempotencyKey: createIdempotencyKey(), fingerprint },
    replaysExisting: false,
  };
}

export function KakaoFriendShareControls({
  connector,
  onReload,
  secondaryActionClass,
  secondaryActionStyle,
  tr,
}: KakaoFriendShareControlsProps) {
  const [busyAction, setBusyAction] = useState<"connect" | "disconnect" | null>(null);
  const [composerOpen, setComposerOpen] = useState(false);
  const [friendsPage, setFriendsPage] = useState<KakaoFriendsPage | null>(null);
  const [friendsLoading, setFriendsLoading] = useState(false);
  const [selected, setSelected] = useState<Set<string>>(() => new Set());
  const [text, setText] = useState("");
  const [sending, setSending] = useState(false);
  const [duplicateRiskPending, setDuplicateRiskPending] = useState(false);
  const [pendingIntent, setPendingIntent] = useState<PendingSendIntent | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<KakaoSendResult | null>(null);
  const [accountId, setAccountId] = useState("");
  const friendRequestVersion = useRef(0);
  const accounts: KakaoAccountSummary[] = connector.connection?.accounts ?? [];
  const selectedAccount = accounts.find((account) => account.account_id === accountId) ?? null;

  const actions = new Set(connector.actions ?? []);
  const canConnect = actions.has("connect") || actions.has("reconnect");
  const canTestSend = accounts.some((account) => account.status === "active");
  const charCount = Array.from(text).length;
  const sendDisabled = sending || !selectedAccount || selected.size === 0 || selected.size > 5 || text.trim().length === 0 || charCount > 200;
  const memoSendDisabled = sending || !selectedAccount || text.trim().length === 0 || charCount > 200;
  const friends = useMemo(() => friendsPage?.friends ?? [], [friendsPage]);
  const currentFingerprint = useMemo(() => kakaoSendIntentFingerprint(accountId, selected, text), [accountId, selected, text]);
  const memoFingerprint = useMemo(() => kakaoMemoIntentFingerprint(accountId, text), [accountId, text]);
  const safelyReplaysCurrentIntent = pendingIntent?.fingerprint === currentFingerprint;
  const safelyReplaysMemoIntent = pendingIntent?.fingerprint === memoFingerprint;

  const connect = async () => {
    setBusyAction("connect");
    setError(null);
    try {
      const response = await api.startKakaoOAuth();
      const target = new URL(response.authorize_url);
      if (!isAllowedKakaoAuthorizeUrl(target)) {
        throw new Error("unexpected-oauth-origin");
      }
      window.location.assign(target.toString());
    } catch {
      setError(tr("카카오 연결을 시작하지 못했습니다.", "Failed to start Kakao connection."));
      setBusyAction(null);
    }
  };

  const disconnect = async (targetAccountId: string) => {
    const confirmed = window.confirm(
      tr(
        "AgentDesk에 저장된 카카오 연결을 해제할까요? 카카오 계정의 앱 동의는 원격으로 철회되지 않습니다.",
        "Remove the locally stored Kakao connection? This does not revoke the app grant in Kakao.",
      ),
    );
    if (!confirmed) return;
    setBusyAction("disconnect");
    setError(null);
    try {
      await api.disconnectKakao(targetAccountId);
      setComposerOpen(false);
      setFriendsPage(null);
      setSelected(new Set());
      setResult(null);
      setDuplicateRiskPending(false);
      setPendingIntent(null);
      onReload();
    } catch {
      setError(tr("카카오 연결을 해제하지 못했습니다.", "Failed to disconnect Kakao."));
    } finally {
      setBusyAction(null);
    }
  };

  const loadFriends = async (offset = 0, append = false) => {
    if (!accountId) return;
    const requestVersion = ++friendRequestVersion.current;
    setFriendsLoading(true);
    setError(null);
    try {
      const page = await api.getKakaoFriends(accountId, offset, 20);
      if (requestVersion !== friendRequestVersion.current) return;
      if (!append) {
        setSelected(new Set());
        setResult(null);
      }
      setFriendsPage((previous) => {
        if (!append || !previous) return page;
        const merged = new Map<string, KakaoFriendView>();
        for (const friend of [...previous.friends, ...page.friends]) merged.set(friend.uuid, friend);
        return { ...page, friends: [...merged.values()] };
      });
    } catch {
      setError(tr("메시지를 보낼 수 있는 친구 목록을 불러오지 못했습니다.", "Failed to load message-eligible friends."));
    } finally {
      setFriendsLoading(false);
    }
  };

  const openComposer = () => {
    const next = !composerOpen;
    setComposerOpen(next);
    setResult(null);
    setError(null);
    if (next) {
      setAccountId("");
    } else {
      setFriendsPage(null);
      setSelected(new Set());
      setText("");
      setPendingIntent(null);
    }
  };

  const selectAccount = (nextAccountId: string) => {
    friendRequestVersion.current += 1;
    setAccountId(nextAccountId);
    setFriendsPage(null);
    setSelected(new Set());
    setResult(null);
    setDuplicateRiskPending(false);
    setPendingIntent(null);
  };

  const toggleRecipient = (uuid: string) => {
    setResult(null);
    setSelected((current) => {
      const next = new Set(current);
      if (next.has(uuid)) {
        next.delete(uuid);
      } else if (next.size < 5) {
        next.add(uuid);
      }
      return next;
    });
  };

  const send = async () => {
    if (sendDisabled) return;
    if (duplicateRiskPending && !safelyReplaysCurrentIntent) {
      const acceptsDuplicateRisk = window.confirm(
        tr(
          "이전 요청은 이미 전달되었을 수 있습니다. 새 요청으로 다시 보내면 중복 메시지가 생길 수 있습니다. 계속할까요?",
          "The previous request may already have delivered. Sending a new request can create a duplicate. Continue?",
        ),
      );
      if (!acceptsDuplicateRisk) return;
    }
    const confirmed = window.confirm(
      safelyReplaysCurrentIntent
        ? tr(
          "같은 요청 키로 기존 전송 결과를 다시 확인할까요? 서버가 이 키로 새 전송을 시작하지 않았다면 한 번만 전송합니다.",
          "Check the existing send with the same request key? The server sends once only if this key never started a send.",
        )
        : tr(
          `선택한 ${selected.size}명에게 지금 한 번 전송할까요? 자동 재전송은 하지 않습니다.`,
          `Send once to ${selected.size} selected friend(s)? AgentDesk will not retry automatically.`,
        ),
    );
    if (!confirmed) return;
    setSending(true);
    setError(null);
    setResult(null);
    const resolvedIntent = resolveKakaoSendIntent(
      pendingIntent,
      currentFingerprint,
      () => crypto.randomUUID(),
    );
    setPendingIntent(resolvedIntent.intent);
    try {
      const response = await api.sendKakaoFriendMessage(
        resolvedIntent.intent.idempotencyKey,
        accountId,
        [...selected],
        text,
      );
      setResult(response);
      const hasDuplicateRisk = response.status === "unknown" || response.status === "partial_success";
      setDuplicateRiskPending(hasDuplicateRisk);
      if (!hasDuplicateRisk) setPendingIntent(null);
    } catch {
      setDuplicateRiskPending(true);
      setError(
        tr(
          "요청 결과를 확인하지 못했습니다. 중복 가능성이 있으므로 같은 내용을 바로 다시 보내지 마세요.",
          "The request result could not be confirmed. Do not immediately resend because delivery may have occurred.",
        ),
      );
    } finally {
      setSending(false);
    }
  };

  const sendToMe = async () => {
    if (memoSendDisabled) return;
    if (duplicateRiskPending && !safelyReplaysMemoIntent) {
      const acceptsDuplicateRisk = window.confirm(
        tr(
          "이전 요청은 이미 전달되었을 수 있습니다. 새 요청으로 다시 보내면 중복 메시지가 생길 수 있습니다. 계속할까요?",
          "The previous request may already have delivered. Sending a new request can create a duplicate. Continue?",
        ),
      );
      if (!acceptsDuplicateRisk) return;
    }
    const confirmed = window.confirm(
      safelyReplaysMemoIntent
        ? tr(
          "같은 요청 키로 기존 나에게 보내기 결과를 다시 확인할까요? 새 전송은 시작하지 않습니다.",
          "Check the existing self-send with the same request key? No new send will start.",
        )
        : tr(
          "내 카카오톡 나와의 채팅방으로 지금 한 번 전송할까요? 자동 재전송은 하지 않습니다.",
          "Send once to your Kakao My Chatroom? AgentDesk will not retry automatically.",
        ),
    );
    if (!confirmed) return;
    setSending(true);
    setError(null);
    setResult(null);
    const resolvedIntent = resolveKakaoSendIntent(
      pendingIntent,
      memoFingerprint,
      () => crypto.randomUUID(),
    );
    setPendingIntent(resolvedIntent.intent);
    try {
      const response = await api.sendKakaoMemoMessage(resolvedIntent.intent.idempotencyKey, accountId, text);
      setResult(response);
      const hasDuplicateRisk = response.status === "unknown" || response.status === "partial_success";
      setDuplicateRiskPending(hasDuplicateRisk);
      if (!hasDuplicateRisk) setPendingIntent(null);
    } catch {
      setDuplicateRiskPending(true);
      setError(
        tr(
          "나에게 보내기 결과를 확인하지 못했습니다. 중복 가능성이 있으므로 같은 내용을 바로 다시 보내지 마세요.",
          "The self-send result could not be confirmed. Do not immediately resend because delivery may have occurred.",
        ),
      );
    } finally {
      setSending(false);
    }
  };

  return (
    <div className="mt-4 border-t pt-4" style={{ borderColor: "color-mix(in srgb, var(--th-border) 62%, transparent)" }}>
      {connector.connection?.landing_url ? (
        <div className="mb-3 text-xs leading-5" style={{ color: "var(--th-text-muted)" }}>
          <span>{tr("메시지에 포함되는 고정 링크: ", "Fixed link included in the message: ")}</span>
          <a href={connector.connection.landing_url} target="_blank" rel="noreferrer" className="break-all underline" style={{ color: "var(--th-text)" }}>
            {connector.connection.landing_url}
          </a>
        </div>
      ) : null}
      <div className="flex flex-wrap gap-2">
        {canConnect ? (
          <button
            type="button"
            className={secondaryActionClass}
            style={secondaryActionStyle}
            disabled={busyAction !== null}
            onClick={() => void connect()}
          >
            {busyAction === "connect" ? <Loader2 size={13} className="animate-spin" /> : <Link2 size={13} />}
            {connector.connection?.state === "not_connected"
              ? tr("카카오 연결", "Connect Kakao")
              : tr("다시 연결", "Reconnect")}
          </button>
        ) : null}
        {canTestSend ? (
          <button
            type="button"
            className={secondaryActionClass}
            style={secondaryActionStyle}
            onClick={openComposer}
          >
            <MessageSquareShare size={13} />
            {composerOpen ? tr("시험 발송 닫기", "Close test send") : tr("시험 발송", "Test send")}
          </button>
        ) : null}
      </div>

      {error ? (
        <div className="mt-3 rounded-xl border px-3 py-2 text-xs leading-5" style={{ borderColor: "rgba(248, 113, 113, 0.38)", background: "rgba(248, 113, 113, 0.10)", color: "var(--th-text)" }}>
          {error}
        </div>
      ) : null}

      {accounts.length > 0 ? (
        <div className="mt-3 space-y-2" data-testid="kakao-account-list">
          {accounts.map((account) => (
            <div key={account.account_id} className="flex items-center justify-between rounded-lg border px-3 py-2 text-xs" style={{ borderColor: "var(--th-border)", color: "var(--th-text)" }}>
              <span>{tr("카카오 계정", "Kakao account")} · {account.is_legacy ? "primary" : account.account_id.slice(0, 8)} · {account.status}</span>
              <button type="button" className={secondaryActionClass} style={secondaryActionStyle} disabled={busyAction !== null} onClick={() => void disconnect(account.account_id)} aria-label={tr("계정 연결 해제", "Disconnect account")}>
                {busyAction === "disconnect" ? <Loader2 size={13} className="animate-spin" /> : <Unlink size={13} />} ×
              </button>
            </div>
          ))}
        </div>
      ) : null}

      {composerOpen ? (
        <div className="mt-4 space-y-4" data-testid="kakao-friend-share-composer">
          <label className="block">
            <div className="mb-2 text-xs" style={{ color: "var(--th-text-muted)" }}>{tr("발신 카카오 계정", "Sender Kakao account")}</div>
            <select value={accountId} onChange={(event) => selectAccount(event.target.value)} className="w-full rounded-xl border px-3 py-2 text-sm" style={{ borderColor: "var(--th-border)", background: "var(--th-bg-surface)", color: "var(--th-text)" }}>
              <option value="">{tr("계정을 선택하세요", "Select an account")}</option>
              {accounts.filter((account) => account.status === "active").map((account) => <option key={account.account_id} value={account.account_id}>{account.is_legacy ? "primary" : account.account_id.slice(0, 8)}</option>)}
            </select>
          </label>
          <div>
            <div className="mb-2 flex items-center justify-between text-xs" style={{ color: "var(--th-text-muted)" }}>
              <span>{tr(`수신자 ${selected.size}/5`, `Recipients ${selected.size}/5`)}</span>
              <button type="button" className={secondaryActionClass} style={secondaryActionStyle} disabled={friendsLoading} onClick={() => void loadFriends()}>
                {friendsLoading ? <Loader2 size={12} className="animate-spin" /> : null}
                {tr("새로고침", "Refresh")}
              </button>
            </div>
            <div className="max-h-52 space-y-2 overflow-y-auto rounded-xl border p-3" style={{ borderColor: "color-mix(in srgb, var(--th-border) 62%, transparent)" }}>
              {friendsLoading && friends.length === 0 ? (
                <div className="text-xs" style={{ color: "var(--th-text-muted)" }}>{tr("친구를 불러오는 중입니다.", "Loading friends.")}</div>
              ) : friends.length === 0 ? (
                <div className="text-xs leading-5" style={{ color: "var(--th-text-muted)" }}>{tr("메시지 수신 동의를 완료한 친구가 없습니다.", "No message-eligible friends are available.")}</div>
              ) : friends.map((friend) => (
                <label key={friend.uuid} className="flex cursor-pointer items-center gap-3 rounded-lg px-2 py-1.5 text-sm" style={{ color: "var(--th-text)" }}>
                  <input type="checkbox" checked={selected.has(friend.uuid)} disabled={!selected.has(friend.uuid) && selected.size >= 5} onChange={() => toggleRecipient(friend.uuid)} />
                  <span>{friend.display_name || tr("이름 없는 친구", "Unnamed friend")}</span>
                </label>
              ))}
              {friendsPage?.next_offset !== null && friendsPage?.next_offset !== undefined ? (
                <button type="button" className={secondaryActionClass} style={secondaryActionStyle} disabled={friendsLoading} onClick={() => void loadFriends(friendsPage.next_offset ?? 0, true)}>
                  {tr("친구 더 불러오기", "Load more friends")}
                </button>
              ) : null}
            </div>
          </div>

          <label className="block">
            <div className="mb-2 flex items-center justify-between text-xs" style={{ color: "var(--th-text-muted)" }}>
              <span>{tr("메시지", "Message")}</span>
              <span>{charCount}/200</span>
            </div>
            <textarea
              value={text}
              onChange={(event) => {
                setText(event.target.value);
                setResult(null);
              }}
              maxLength={400}
              rows={4}
              className="w-full resize-y rounded-xl border px-3 py-2 text-sm outline-none"
              style={{ borderColor: charCount > 200 ? "rgba(248, 113, 113, 0.62)" : "var(--th-border)", background: "var(--th-bg-surface)", color: "var(--th-text)" }}
              placeholder={tr("200자 이하의 텍스트를 입력하세요.", "Enter up to 200 characters.")}
            />
          </label>

          <button type="button" className={secondaryActionClass} style={secondaryActionStyle} disabled={sendDisabled} onClick={() => void send()}>
            {sending ? <Loader2 size={13} className="animate-spin" /> : <Send size={13} />}
            {sending
              ? tr("한 번 전송 중...", "Sending once...")
              : safelyReplaysCurrentIntent
                ? tr("같은 요청 결과 다시 확인", "Check the same request")
                : tr("선택한 친구에게 지금 전송", "Send now to selected friends")}
          </button>

          <button type="button" className={secondaryActionClass} style={secondaryActionStyle} disabled={memoSendDisabled} onClick={() => void sendToMe()}>
            {sending ? <Loader2 size={13} className="animate-spin" /> : <Send size={13} />}
            {sending
              ? tr("한 번 전송 중...", "Sending once...")
              : safelyReplaysMemoIntent
                ? tr("나에게 보낸 같은 요청 결과 확인", "Check the same self-send")
                : tr("나에게 지금 전송", "Send now to me")}
          </button>

          {result ? <KakaoSendOutcome result={result} tr={tr} /> : null}
        </div>
      ) : null}
    </div>
  );
}

function KakaoSendOutcome({ result, tr }: { result: KakaoSendResult; tr: (ko: string, en: string) => string }) {
  const isUnknown = result.status === "unknown";
  const message = result.status === "success"
    ? tr("전송 요청이 모두 성공했습니다.", "All send requests succeeded.")
    : result.status === "partial_success"
      ? tr("일부 친구에게만 전송되었습니다.", "The message was sent to some friends only.")
      : result.status === "failed"
        ? tr("전송이 거절되었습니다.", "The provider rejected the send.")
        : tr("전달 여부를 확인할 수 없습니다. 자동 재전송하지 않습니다.", "Delivery is unknown. AgentDesk will not retry automatically.");
  return (
    <div className="rounded-xl border px-3 py-3 text-xs leading-5" style={{ borderColor: isUnknown ? "rgba(251, 191, 36, 0.45)" : "rgba(34, 197, 94, 0.36)", background: isUnknown ? "rgba(251, 191, 36, 0.10)" : "rgba(34, 197, 94, 0.10)", color: "var(--th-text)" }}>
      <div className="font-semibold">{message}</div>
      <div className="mt-1" style={{ color: "var(--th-text-muted)" }}>
        {tr(
          `요청 ${result.requested_count} · 성공 ${result.successful_count} · 실패 ${result.failed_count}`,
          `Requested ${result.requested_count} · succeeded ${result.successful_count} · failed ${result.failed_count}`,
        )}
      </div>
    </div>
  );
}
