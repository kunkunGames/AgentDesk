import { Suspense, lazy } from "react";
import * as Dialog from "@radix-ui/react-dialog";

const OnboardingWizard = lazy(() => import("../OnboardingWizard"));

type SettingsOnboardingOverlayProps = {
  isKo: boolean;
  onClose: () => void;
  open: boolean;
  tr: (ko: string, en: string) => string;
};

export function SettingsOnboardingOverlay({
  isKo,
  onClose,
  open,
  tr,
}: SettingsOnboardingOverlayProps) {
  return (
    <Dialog.Root open={open} onOpenChange={(isOpen) => { if (!isOpen) onClose(); }}>
      <Dialog.Portal>
        <Dialog.Overlay className="fixed inset-0 z-50 bg-[#0a0e1a]" />
        <Dialog.Content
          className="fixed inset-0 z-50 overflow-y-auto"
          aria-label={tr("온보딩 마법사", "Onboarding wizard")}
        >
          <div className="flex min-h-screen items-start justify-center pb-16 pt-8">
            <div className="w-full max-w-2xl">
              <div className="mb-2 flex justify-end px-4">
                <Dialog.Close asChild>
                  <button
                    className="min-h-[44px] rounded-lg border px-4 py-2.5 text-sm focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[color:var(--th-accent-primary)] focus-visible:ring-offset-2 focus-visible:ring-offset-[#0a0e1a]"
                    style={{ borderColor: "rgba(148,163,184,0.3)", color: "var(--th-text-muted)" }}
                  >
                    x {tr("닫기", "Close")}
                  </button>
                </Dialog.Close>
              </div>
              <Suspense fallback={<div className="py-8 text-center" style={{ color: "var(--th-text-muted)" }}>Loading...</div>}>
                <OnboardingWizard
                  isKo={isKo}
                  onComplete={() => {
                    onClose();
                    window.location.reload();
                  }}
                />
              </Suspense>
            </div>
          </div>
        </Dialog.Content>
      </Dialog.Portal>
    </Dialog.Root>
  );
}
