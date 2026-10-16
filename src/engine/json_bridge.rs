//! JSON values crossing the policy engine JavaScript boundary.

use anyhow::Result;

pub(super) fn json_to_js<'js>(
    ctx: &rquickjs::Ctx<'js>,
    val: &serde_json::Value,
) -> Result<rquickjs::Value<'js>> {
    match val {
        serde_json::Value::Null => Ok(rquickjs::Value::new_null(ctx.clone())),
        serde_json::Value::Bool(b) => Ok(rquickjs::Value::new_bool(ctx.clone(), *b)),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(rquickjs::Value::new_int(ctx.clone(), i as i32))
            } else if let Some(f) = n.as_f64() {
                Ok(rquickjs::Value::new_float(ctx.clone(), f))
            } else {
                Ok(rquickjs::Value::new_null(ctx.clone()))
            }
        }
        serde_json::Value::String(s) => {
            let js_str = rquickjs::String::from_str(ctx.clone(), s)
                .map_err(|e| anyhow::anyhow!("string conversion: {e}"))?;
            Ok(js_str.into())
        }
        serde_json::Value::Array(arr) => {
            let js_arr = rquickjs::Array::new(ctx.clone())
                .map_err(|e| anyhow::anyhow!("array creation: {e}"))?;
            for (i, item) in arr.iter().enumerate() {
                let js_item = json_to_js(ctx, item)?;
                js_arr
                    .set(i, js_item)
                    .map_err(|e| anyhow::anyhow!("array set: {e}"))?;
            }
            Ok(js_arr.into_value())
        }
        serde_json::Value::Object(map) => {
            let obj = rquickjs::Object::new(ctx.clone())
                .map_err(|e| anyhow::anyhow!("object creation: {e}"))?;
            for (k, v) in map {
                let js_v = json_to_js(ctx, v)?;
                obj.set(&**k, js_v)
                    .map_err(|e| anyhow::anyhow!("object set: {e}"))?;
            }
            Ok(obj.into_value())
        }
    }
}
