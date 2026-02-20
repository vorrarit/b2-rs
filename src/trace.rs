use time::{UtcOffset, format_description};
use tracing_subscriber::fmt;

pub fn trace_init() {
    let timer_format: Vec<format_description::BorrowedFormatItem<'_>> = format_description::parse(
        "[year]-[month padding:zero]-[day padding:zero] [hour]:[minute]:[second].[subsecond digits:3]",
    )
    .expect("Failed to parse time format");

    let time_offset = UtcOffset::current_local_offset()
        .unwrap_or_else(|_| UtcOffset::UTC); // Fallback to UTC if local offset can't be determined

    let timer = fmt::time::OffsetTime::new(time_offset, timer_format);

    // let subscriber = Registry::default()
    //     .with(EnvFilter::try_from_default_env().unwrap_or(EnvFilter::new("info")))
    //     .with(tracing_subscriber::fmt::layer().pretty()
    //         .with_target(false)
    //         .with_file(false)
    //         .with_line_number(false)
    //         .with_timer(timer)
    //     );
    //     // .with(HierarchicalLayer::default());


    // let _ = tracing::subscriber::set_global_default(subscriber);
    tracing_subscriber::fmt::fmt().with_timer(timer).with_target(false).init();
}
