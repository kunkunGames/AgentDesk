with open('src/server/worker_registry.rs', 'r') as f:
    data = f.read()
data = data.replace('target = spec.target,\n            kind = spec.kind.as_doc_str(),\n            stage = spec.start_stage.as_doc_str(),', 'target = spec.target,\n            kind = spec.kind.as_doc_str(),\n            stage = spec.start_stage.as_doc_str(),')
with open('src/server/worker_registry.rs', 'w') as f:
    f.write(data)
