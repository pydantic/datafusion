use datafusion::{common::Result, prelude::*};
use datafusion_proto::bytes::{physical_plan_from_bytes, physical_plan_to_bytes};
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};


#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[allow(non_upper_case_globals)]
#[export_name = "malloc_conf"]
pub static malloc_conf: &[u8] = b"prof:true,prof_active:true,lg_prof_sample:19\0";


#[tokio::main]
async fn main() -> Result<()> {
    let mut prof_ctl = jemalloc_pprof::PROF_CTL.as_ref().unwrap().lock().await;
    prof_ctl.activate().unwrap();

    let _plan = {
        let ctx = SessionContext::new();
        let batches = ctx.sql("SELECT c FROM generate_series(1, 1000000) t(c)").await?.collect().await?;
        let file = std::fs::File::create("test.parquet")?;
        let props = WriterProperties::builder()
            // limit batch sizes so that we have useful statistics
            .set_max_row_group_size(4096)
            .build();
        let mut writer = ArrowWriter::try_new(file, batches[0].schema(), Some(props))?;
        for batch in &batches {
            writer.write(batch)?;
        }
        writer.close()?;

        let mut df = ctx.read_parquet("test.parquet", ParquetReadOptions::default()).await?;
        df = df.filter(col("c").in_list((1_000..10_000).map(|v| lit(v)).collect(), false))?;
        let plan = df.create_physical_plan().await?;
        physical_plan_from_bytes(&physical_plan_to_bytes(plan)?, &ctx.task_ctx())?
    };

    let pprof = prof_ctl.dump_pprof().unwrap();
    std::fs::write("proto_memory.pprof", pprof).unwrap();

    Ok(())
}
