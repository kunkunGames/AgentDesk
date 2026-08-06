import sys

def patch():
    file_path = "src/server/routes/dispatches/crud.rs"
    with open(file_path, "r") as f:
        content = f.read()

    search = """    let options = crate::dispatch::DispatchCreateOptions {
        skip_outbox: body.skip_outbox.unwrap_or(false),
        sidecar_dispatch: context
            .get("sidecar_dispatch")
            .and_then(|value| value.as_bool())
            .unwrap_or(false)
            || context
                .get("phase_gate")
                .and_then(|value| value.as_object())
                .is_some(),
    };"""
    replace = """    let options = crate::dispatch::DispatchCreateOptions {
        skip_outbox: body.skip_outbox.unwrap_or(false),
        sidecar_dispatch: crate::dispatch::dispatch_context_requests_sidecar(&context),
    };"""

    if search in content:
        with open(file_path, "w") as f:
            f.write(content.replace(search, replace))
        print("Patched successfully")
    else:
        print("Failed to patch, search string not found")
        sys.exit(1)

patch()
