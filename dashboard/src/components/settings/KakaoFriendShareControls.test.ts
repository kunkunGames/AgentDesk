import { describe, expect, it } from "vitest";
import {
  isAllowedKakaoAuthorizeUrl,
  kakaoAccountCanSendMemo,
  kakaoAccountCanSendToFriends,
  kakaoMemoIntentFingerprint,
  kakaoSendIntentFingerprint,
  resolveKakaoSendIntent,
} from "./KakaoFriendShareControls";

describe("Kakao friend share safety helpers", () => {
  it("keeps talk-message-only accounts available for self-send but not friend send", () => {
    const account = {
      account_id: "account-a",
      status: "consent_incomplete",
      scopes: ["talk_message"],
      access_expires_at: null,
      is_legacy: false,
    };
    expect(kakaoAccountCanSendMemo(account)).toBe(true);
    expect(kakaoAccountCanSendToFriends(account)).toBe(false);
    expect(kakaoAccountCanSendToFriends({
      ...account,
      status: "active",
      scopes: ["friends", "talk_message"],
    })).toBe(true);
  });

  it("keeps self-send idempotency separate from friend-recipient payloads", () => {
    expect(kakaoMemoIntentFingerprint("account-a", "hello")).not.toBe(
      kakaoSendIntentFingerprint("account-a", [], "hello"),
    );
  });

  it("uses a recipient-order-independent fingerprint that remains bound to text", () => {
    const first = kakaoSendIntentFingerprint("account-a", ["friend-b", "friend-a"], "hello");
    const reordered = kakaoSendIntentFingerprint("account-a", ["friend-a", "friend-b"], "hello");
    const changed = kakaoSendIntentFingerprint("account-a", ["friend-a", "friend-b"], "changed");

    expect(first).toBe(reordered);
    expect(first).not.toBe(changed);
    expect(first).not.toBe(kakaoSendIntentFingerprint("account-b", ["friend-a", "friend-b"], "hello"));
    expect(first).not.toBe(kakaoSendIntentFingerprint("account-a", ["friend-a", "friend-b"], "hello", "https://example.com/thumbnail.jpg"));
  });

  it("reuses an in-memory idempotency key only for the identical payload", () => {
    const pending = { idempotencyKey: "existing-key", fingerprint: "payload-a" };
    const createKey = () => "new-key";

    expect(resolveKakaoSendIntent(pending, "payload-a", createKey)).toEqual({
      intent: pending,
      replaysExisting: true,
    });
    expect(resolveKakaoSendIntent(pending, "payload-b", createKey)).toEqual({
      intent: { idempotencyKey: "new-key", fingerprint: "payload-b" },
      replaysExisting: false,
    });
  });

  it("accepts only the exact Kakao HTTPS authorization endpoint", () => {
    expect(
      isAllowedKakaoAuthorizeUrl(
        new URL("https://kauth.kakao.com/oauth/authorize?client_id=redacted"),
      ),
    ).toBe(true);
    for (const unsafe of [
      "http://kauth.kakao.com/oauth/authorize",
      "https://kauth.kakao.com:444/oauth/authorize",
      "https://user@kauth.kakao.com/oauth/authorize",
      "https://kauth.kakao.com/oauth/token",
      "https://kauth.kakao.com.evil.example/oauth/authorize",
    ]) {
      expect(isAllowedKakaoAuthorizeUrl(new URL(unsafe))).toBe(false);
    }
  });
});
