// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	common::CommitVersion,
	interface::catalog::{id::ViewId, object::ObjectId},
};
use reifydb_flow::transaction::frontier::*;
use reifydb_value::factory::time::at_millis;

const OUTPUT: ObjectId = ObjectId::View(ViewId(42));

fn entry(frontier: u64, at: u64) -> FrontierEntry {
	FrontierEntry {
		output: OUTPUT,
		frontier: at_millis(frontier),
		at: CommitVersion(at),
	}
}

#[test]
fn a_lower_publish_never_regresses_the_frontier_or_its_stamp() {
	// A regression re-opens a sealed horizon, and a rejected publish must never raise the stamp.
	let frontiers = OutputFrontiers::default();

	frontiers.publish(OUTPUT, at_millis(5_000), CommitVersion(10));
	frontiers.publish(OUTPUT, at_millis(3_000), CommitVersion(20));

	assert_eq!(frontiers.resolve(OUTPUT, CommitVersion(30)), Frontier::Visible(at_millis(5_000)));
	assert_eq!(
		frontiers.resolve(OUTPUT, CommitVersion(15)),
		Frontier::Visible(at_millis(5_000)),
		"the rejected publish must not have carried its stamp forward"
	);
}

#[test]
fn a_frontier_published_at_the_readers_own_version_is_withheld() {
	// At an equal version nothing orders producer before consumer, so folding it seals early.
	let frontiers = OutputFrontiers::default();

	frontiers.publish(OUTPUT, at_millis(5_000), CommitVersion(10));

	assert_eq!(frontiers.resolve(OUTPUT, CommitVersion(10)), Frontier::Withheld);
	assert_eq!(frontiers.resolve(OUTPUT, CommitVersion(11)), Frontier::Visible(at_millis(5_000)));
}

#[test]
fn a_withheld_frontier_never_reads_as_one_that_was_never_published() {
	// A tie is routine, so conflating the two makes the unpublished-source warning pure noise.
	let frontiers = OutputFrontiers::default();

	assert_eq!(frontiers.resolve(OUTPUT, CommitVersion(10)), Frontier::Unpublished);

	frontiers.publish(OUTPUT, at_millis(5_000), CommitVersion(10));

	assert_eq!(frontiers.resolve(OUTPUT, CommitVersion(10)), Frontier::Withheld);
}

#[test]
fn an_object_that_never_published_stays_distinguishable_from_one_that_published_the_epoch() {
	// Collapsing the two into zero is exactly why the pin warning cannot fire when stale.
	let frontiers = OutputFrontiers::default();

	assert_eq!(frontiers.resolve(OUTPUT, CommitVersion(10)), Frontier::Unpublished);

	frontiers.publish(OUTPUT, at_millis(0), CommitVersion(1));

	assert_eq!(frontiers.resolve(OUTPUT, CommitVersion(10)), Frontier::Visible(at_millis(0)));
}

#[test]
fn a_published_entry_is_reported_for_persistence_at_the_stamp_it_was_published_with() {
	// A sweep writes whatever entries() reports, so a wrong stamp resurrects a stale frontier on restart.
	let frontiers = OutputFrontiers::default();

	assert!(frontiers.entries().is_empty());

	frontiers.publish(OUTPUT, at_millis(5_000), CommitVersion(10));
	assert_eq!(frontiers.entries(), vec![entry(5_000, 10)]);

	frontiers.publish(OUTPUT, at_millis(9_000), CommitVersion(20));
	assert_eq!(frontiers.entries(), vec![entry(9_000, 20)]);
}

#[test]
fn a_hydrated_entry_is_readable_but_never_needs_persisting() {
	// Hydrated rows are already in the store, so counting them as unpersisted rewrites them on every restart.
	let frontiers = OutputFrontiers::default();

	frontiers.hydrate(vec![entry(5_000, 10)]);

	assert_eq!(frontiers.entries(), vec![entry(5_000, 10)]);
	assert!(frontiers.unpersisted().is_none());
	assert_eq!(frontiers.resolve(OUTPUT, CommitVersion(11)), Frontier::Withheld);
}

#[test]
fn a_second_publish_at_the_same_frontier_still_needs_persisting() {
	// The generation counts publishes, not values; a no-op publish must not mask a real one behind it.
	let frontiers = OutputFrontiers::default();

	frontiers.publish(OUTPUT, at_millis(5_000), CommitVersion(10));
	let (generation, _) = frontiers.unpersisted().expect("a publish must be unpersisted");
	frontiers.mark_persisted(generation);

	frontiers.publish(OUTPUT, at_millis(3_000), CommitVersion(20));

	assert!(
		frontiers.unpersisted().is_some(),
		"a rejected lower publish still bumps the generation, so the next sweep is a cheap no-op write"
	);
}

#[test]
fn a_hydrated_frontier_is_never_foldable_before_its_producer_publishes() {
	// The rows justifying a persisted frontier can be lost with the commit buffer, so folding it
	// unpublished seals a window early.
	let frontiers = OutputFrontiers::default();

	frontiers.hydrate(vec![entry(9_000, 10)]);

	assert_eq!(frontiers.resolve(OUTPUT, CommitVersion(u64::MAX)), Frontier::Withheld);
}

#[test]
fn max_wins_resumes_once_a_hydrated_entry_has_been_superseded() {
	// Superseding must apply exactly once, otherwise every later publish overwrites a higher live frontier.
	let frontiers = OutputFrontiers::default();

	frontiers.hydrate(vec![entry(9_000, 10)]);
	frontiers.publish(OUTPUT, at_millis(3_000), CommitVersion(20));
	frontiers.publish(OUTPUT, at_millis(1_000), CommitVersion(30));

	assert_eq!(frontiers.resolve(OUTPUT, CommitVersion(31)), Frontier::Visible(at_millis(3_000)));
}

#[test]
fn a_live_publish_supersedes_an_unclamped_hydrated_value_rather_than_maxing_against_it() {
	// A live publish is justified and the hydrated value is not, so max-wins here would keep an unjustified
	// frontier forever.
	let frontiers = OutputFrontiers::default();

	frontiers.hydrate(vec![entry(9_000, 10)]);
	frontiers.publish(OUTPUT, at_millis(3_000), CommitVersion(20));

	assert_eq!(
		frontiers.resolve(OUTPUT, CommitVersion(21)),
		Frontier::Visible(at_millis(3_000)),
		"the untrusted hydrated value must not survive a live publish"
	);
}

#[test]
fn a_hydrated_frontier_stays_withheld_until_the_reader_passes_its_stamp() {
	// A hydrated stamp must stay withheld until the flow replays past it, or the restart seals early.
	let frontiers = OutputFrontiers::default();

	frontiers.hydrate(vec![entry(5_000, 500)]);
	frontiers.publish(OUTPUT, at_millis(5_000), CommitVersion(500));

	assert_eq!(frontiers.resolve(OUTPUT, CommitVersion(499)), Frontier::Withheld);
	assert_eq!(frontiers.resolve(OUTPUT, CommitVersion(500)), Frontier::Withheld);
	assert_eq!(frontiers.resolve(OUTPUT, CommitVersion(501)), Frontier::Visible(at_millis(5_000)));
}
