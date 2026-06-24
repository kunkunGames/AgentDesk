use sqlx::{Postgres, QueryBuilder};
fn main() {
    let mut builder: QueryBuilder<Postgres> = QueryBuilder::new(
        "INSERT INTO auto_queue_slots (agent_id, slot_index, thread_id_map) "
    );
    let agent_id = "agent-1";
    let slot_pool_size = 5i64;

    let indices: Vec<i64> = (0..slot_pool_size.clamp(1, 32)).collect();
    builder.push_values(indices, |mut b, index| {
        b.push_bind(agent_id).push_bind(index).push(" '{}'::jsonb ");
    });
    builder.push(" ON CONFLICT (agent_id, slot_index) DO NOTHING");

    println!("{}", builder.sql());
}
