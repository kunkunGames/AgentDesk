import { describe, expect, it } from "vitest";
import {
  isAllowedKakaoAuthorizeUrl,
  kakaoSendIntentFingerprint,
  resolveKakaoSendIntent,
} from "./KakaoFriendShareControls";

describe("Kakao friend share safety helpers", () => {
  it("uses a recipient-order-independent fingerprint that remains bound to text", () => {
    const first = kakaoSendIntentFingerprint(["friend-b", "friend-a"], "hello");
    const reordered = kakaoSendIntentFingerprint(["friend-a", "friend-b"], "hello");
    const changed = kakaoSendIntentFingerprint(["friend-a", "friend-b"], "changed");

    expect(first).toBe(reordered);
    expect(first).not.toBe(changed);
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
