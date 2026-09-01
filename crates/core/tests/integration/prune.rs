use anyhow::Result;
use bytesize::ByteSize;
use jiff::Span;
use rstest::rstest;

use std::sync::Arc;

use rustic_core::{
    BackupOptions, BlobType, CheckOptions, ConfigOptions, Credentials, KeyOptions, LimitOption,
    OpenStatus, PathList, PruneOptions, Repository, RepositoryBackends, RepositoryOptions,
    repofile::{Chunker, SnapshotFile},
};
use rustic_testing::backend::in_memory_backend::InMemoryBackend;

use super::{RepoOpen, TestSource, set_up_repo, tar_gz_testdata};

#[rstest]
fn test_prune(
    tar_gz_testdata: Result<TestSource>,
    set_up_repo: Result<RepoOpen>,
    #[values(
        ConfigOptions::default(),
        ConfigOptions::default()
        .set_chunker(Chunker::FixedSize)
        .set_chunk_size(ByteSize::b(2))
    )]
    opts: ConfigOptions,
    #[values(true, false)] instant_delete: bool,
    #[values(
        LimitOption::Percentage(0),
        LimitOption::Percentage(50),
        LimitOption::Unlimited
    )]
    max_unused: LimitOption,
    #[values(true, false)] fast_repack: bool,
) -> Result<()> {
    // Fixtures
    let (source, mut repo) = (tar_gz_testdata?, set_up_repo?.to_indexed_ids()?);
    _ = repo.apply_config(&opts)?;

    let opts = BackupOptions::default();

    // first backup
    let paths = PathList::from_iter(Some(source.0.path().join("0/0/9")));
    let snapshot1 = repo.backup(&opts, &paths, SnapshotFile::default())?;

    // re-read index
    let repo = repo.to_indexed_ids()?;
    // second backup
    let paths = PathList::from_iter(Some(source.0.path().join("0/0/9/2")));
    let _ = repo.backup(&opts, &paths, SnapshotFile::default())?;

    // re-read index
    let repo = repo.to_indexed_ids()?;
    // third backup
    let paths = PathList::from_iter(Some(source.0.path().join("0/0/9/3")));
    let _ = repo.backup(&opts, &paths, SnapshotFile::default())?;

    // drop index
    let repo = repo.drop_index();
    repo.delete_snapshots(&[snapshot1.id])?;

    // get prune plan
    let prune_opts = PruneOptions::default()
        .instant_delete(instant_delete)
        .max_unused(max_unused)
        .keep_delete(Span::default())
        .fast_repack(fast_repack);
    let plan = repo.prune_plan(&prune_opts)?;
    // TODO: Snapshot-test the plan (currently doesn't impl Serialize)
    // assert_ron_snapshot!("prune", plan);
    repo.prune(&prune_opts, plan)?;

    // run check
    let check_opts = CheckOptions::default().read_data(true);
    repo.check(check_opts)?.is_ok()?;

    if !instant_delete {
        // re-run if we only marked pack files. As keep-delete = 0, they should be removed here
        let plan = repo.prune_plan(&prune_opts)?;
        repo.prune(&prune_opts, plan)?;
        repo.check(check_opts)?.is_ok()?;
    }

    Ok(())
}

fn prune_repo_with_archive_class() -> Result<Repository<OpenStatus>> {
    let be = InMemoryBackend::new().with_archive_class("DEEP_ARCHIVE");
    let be = RepositoryBackends::new(Arc::new(be), None);
    Ok(Repository::new(&RepositoryOptions::default(), &be)?.init(
        &Credentials::password("test"),
        &KeyOptions::default(),
        &ConfigOptions::default(),
    )?)
}

#[rstest]
fn test_prune_archive_class_skips_data_repack_unless_repack_data(
    tar_gz_testdata: Result<TestSource>,
) -> Result<()> {
    let source = tar_gz_testdata?;
    let repo = prune_repo_with_archive_class()?.to_indexed_ids()?;

    let opts = BackupOptions::default();
    let paths = PathList::from_iter(Some(source.0.path().join("0/0/9")));
    let snapshot1 = repo.backup(&opts, &paths, SnapshotFile::default())?;
    let repo = repo.to_indexed_ids()?;
    let paths = PathList::from_iter(Some(source.0.path().join("0/0/9/2")));
    let _ = repo.backup(&opts, &paths, SnapshotFile::default())?;
    let repo = repo.drop_index();
    repo.delete_snapshots(&[snapshot1.id])?;

    let default_opts = PruneOptions::default()
        .max_unused(LimitOption::Percentage(0))
        .max_repack(LimitOption::Unlimited);
    let default_plan = repo.prune_plan(&default_opts)?;
    assert_eq!(
        default_plan.stats.size[BlobType::Data].repack,
        0,
        "archive class should not rewrite mixed data packs by default"
    );

    let repack_opts = PruneOptions::default()
        .max_unused(LimitOption::Percentage(0))
        .max_repack(LimitOption::Unlimited)
        .repack_data(true);
    let repack_plan = repo.prune_plan(&repack_opts)?;
    assert!(
        repack_plan.stats.size[BlobType::Data].repack > 0
            || repack_plan.stats.packs.repack > default_plan.stats.packs.repack,
        "repack_data should allow rewriting data packs; default packs.repack={}, repack_data packs.repack={}, data.repack={}",
        default_plan.stats.packs.repack,
        repack_plan.stats.packs.repack,
        repack_plan.stats.size[BlobType::Data].repack,
    );
    Ok(())
}
