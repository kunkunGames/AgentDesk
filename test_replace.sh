sed -i 's/expand_tilde(&config.audio.transcripts_dir)/crate::voice::utils::expand_tilde(\&config.audio.transcripts_dir)/' src/services/discord/voice_barge_in.rs
