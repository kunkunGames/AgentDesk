import { afterEach, describe, expect, it, vi } from "vitest";

import {
  disconnectKakao,
  getKakaoFriends,
  sendKakaoFriendMessage,
  sendKakaoMemoMessage,
  startKakaoOAuth,
} from "./settingsRuntime";

function mockJsonResponse(body: unknown): Response {
  return {
    ok: true,
    status: 200,
    json: vi.fn().mockResolvedValue(body),
  } as unknown as Response;
}

afterEach(() => {
  vi.unstubAllGlobals();
});

type InvalidResponseCase = [
  name: string,
  invoke: () => Promise<unknown>,
  payload: unknown,
];

const invalidResponseCases: InvalidResponseCase[] = [
  [
    "OAuth",
    () => startKakaoOAuth(),
    { authorize_url: "not-a-url", expires_in_seconds: "600" },
  ],
  [
    "disconnect",
    () => disconnectKakao("primary"),
    {
      ok: true,
      account_id: "primary",
      remote_unlinked: true,
    },
  ],
  [
    "friends",
    () => getKakaoFriends("primary", 40, 20),
    {
      friends: [{ uuid: "", display_name: "" }],
      total_count: -1,
      offset: 40,
      limit: 20,
      next_offset: null,
    },
  ],
  [
    "send",
    () => sendKakaoFriendMessage("idempotency-key", "primary", ["friend-a"], "안녕하세요", "https://example.com/thumb.jpg"),
    {
      request_id: "not-a-uuid",
      status: "success",
      requested_count: 1,
      successful_count: 2,
      failed_count: 0,
      replayed: false,
      delivery_may_have_occurred: true,
      automatic_retry_allowed: true,
    },
  ],
  [
    "memo send",
    () => sendKakaoMemoMessage("idempotency-key", "primary", "안녕하세요", "https://example.com/thumb.jpg"),
    {
      request_id: "not-a-uuid",
      status: "success",
      requested_count: 1,
      successful_count: 2,
      failed_count: 0,
      replayed: false,
      delivery_may_have_occurred: true,
      automatic_retry_allowed: true,
    },
  ],
];

const validNonSuccessResults = [
  {
    request_id: "123e4567-e89b-42d3-a456-426614174001",
    status: "partial_success",
    requested_count: 2,
    successful_count: 1,
    failed_count: 1,
    replayed: false,
    delivery_may_have_occurred: true,
    automatic_retry_allowed: false,
  },
  {
    request_id: "123e4567-e89b-42d3-a456-426614174002",
    status: "failed",
    requested_count: 1,
    successful_count: 0,
    failed_count: 1,
    replayed: false,
    delivery_may_have_occurred: false,
    automatic_retry_allowed: false,
  },
  {
    request_id: "123e4567-e89b-42d3-a456-426614174003",
    status: "unknown",
    requested_count: 1,
    successful_count: 0,
    failed_count: 0,
    replayed: true,
    delivery_may_have_occurred: true,
    automatic_retry_allowed: false,
  },
] as const;

describe("Kakao runtime response contracts", () => {
  it("accepts valid OAuth, disconnect, friends, and send responses", async () => {
    const fetchMock = vi.fn()
      .mockResolvedValueOnce(mockJsonResponse({
        authorize_url: "https://kauth.kakao.com/oauth/authorize?client_id=redacted",
        expires_in_seconds: 600,
      }))
      .mockResolvedValueOnce(mockJsonResponse({
        ok: true,
        account_id: "primary",
        remote_unlinked: false,
      }))
      .mockResolvedValueOnce(mockJsonResponse({
        friends: [{ uuid: "friend-a", display_name: "친구 A" }],
        total_count: 1,
        offset: 0,
        limit: 20,
        next_offset: null,
      }))
      .mockResolvedValueOnce(mockJsonResponse({
        request_id: "123e4567-e89b-42d3-a456-426614174000",
        status: "success",
        requested_count: 1,
        successful_count: 1,
        failed_count: 0,
        replayed: false,
        delivery_may_have_occurred: true,
        automatic_retry_allowed: false,
      }))
      .mockResolvedValueOnce(mockJsonResponse({
        request_id: "123e4567-e89b-42d3-a456-426614174004",
        status: "success",
        requested_count: 1,
        successful_count: 1,
        failed_count: 0,
        replayed: false,
        delivery_may_have_occurred: true,
        automatic_retry_allowed: false,
      }));
    vi.stubGlobal("fetch", fetchMock);

    await expect(startKakaoOAuth()).resolves.toMatchObject({
      expires_in_seconds: 600,
    });
    await expect(disconnectKakao("primary")).resolves.toMatchObject({
      remote_unlinked: false,
    });
    await expect(getKakaoFriends("primary", 0, 20)).resolves.toMatchObject({ total_count: 1 });
    await expect(
      sendKakaoFriendMessage("idempotency-key", "primary", ["friend-a"], "안녕하세요", "https://example.com/thumb.jpg"),
    ).resolves.toMatchObject({
      status: "success",
      automatic_retry_allowed: false,
    });
    await expect(sendKakaoMemoMessage("idempotency-key", "primary", "안녕하세요", "https://example.com/thumb.jpg")).resolves.toMatchObject({
      status: "success",
      automatic_retry_allowed: false,
    });
  });

  it.each(validNonSuccessResults)(
    "accepts a status-consistent $status send response",
    async (payload) => {
      vi.stubGlobal("fetch", vi.fn().mockResolvedValue(mockJsonResponse(payload)));

      await expect(
        sendKakaoFriendMessage("idempotency-key", "primary", ["friend-a"], "안녕하세요"),
      ).resolves.toMatchObject({ status: payload.status });
    },
  );

  it.each(invalidResponseCases)(
    "rejects a malformed %s response at the API boundary",
    async (_name, invoke, payload) => {
      vi.stubGlobal(
        "fetch",
        vi.fn().mockResolvedValue(mockJsonResponse(payload)),
      );

      await expect(invoke()).rejects.toThrow();
    },
  );
});
