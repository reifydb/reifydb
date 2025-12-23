# Bincode is now unmaintained

Due to a doxxing and harassment incident, development on bincode has ceased. No
further releases will be published on crates.io.

As crates.io, unlike many other language-attached package management solutions,
lacks the ability to mark a project as archive or remove the last maintainer,
this final release is being published containing only this README, as well as a
lib.rs containing only a compiler error, to inform potential users of the
maintenance status of this crate.

If you were considering using bincode for a new project or are looking to an
alternative to migrate to, you are encouraged to consider some the following
alternatives, as well as the many other great serialization format crates
available in the rust ecosystem:

- [wincode](https://crates.io/crates/wincode)

  Bincode-compatible alternative

- [postcard](https://crates.io/crates/postcard)

  Similar in spirit and structure to bincode, but a bit differently flavored

- [rkyv](https://crates.io/crates/rkyv)

  Zero copy deserialization, honestly the best option for many of the usecases
  that bincode was intended for, like serializing transient data for
  intra-program message passing.

# What the community can do better

Several tooling factors have lead to bincode's maintenance status being a bigger
issue than it needs to be, including:

- crates.io not having functionality for marking a package unmaintained or a
  process for the crates.io maintainers to transfer ownership of an abandoned
  package to a prospective maintainer

  In many other programming language communities such as haskell and hackage, or
  python and pypi, when the last maintainer of a critical crate just walks away,
  there often an in-band status that identifies that package as having no active
  maintainers, and usually at least _some_ policy in place for the package
  collection maintainers to intervene and transfer ownership of such a package,
  allowing the community to assign a new maintainer to the existing package,
  automatically pointing the rest of the ecosystem to the new sources with no
  intervention required on the consumers part.

  crates.io not having any such functionality or processes creates a pretty
  massive pain point when the last maintainer does walk away, placing the burden
  on _them_, very often an already burnt out volunteer, to select a new
  maintainer. If the existing maintainer does not do this, the only current way
  to continue maintenance of the package is to create a differently-named fork,
  and then go through the massive hassle of not only convincing the entire
  ecosystem to use it, but to settle in on one community preferred fork. This
  creates avoidable tension when maintainers _do_ just walk away, with community
  members being scared to make the fork themselves, both out of fear of the
  amount of work getting the community to adopt it entails, and out of fear of
  making the inevitable fork-fight worse by adding another contestant.

- crates.io not having visibility for its internal source control

  Much of the drama that resulted in the doxxing and harassment was centered
  around the git history being rewritten when the repository was blanked out on
  github and moved to sourcehut. While this is a legitimate cause for some level
  of concern that merits at least asking a question, it is disappointing that
  the community is focusing on it so much when virtually none of the users of
  bincode are pulling from the git version anyway, with almost all of the users
  using the crates.io release. While the existing bincode releases are tied to
  the previous history through the git commit reference cargo release's
  `.cargo_vcs_info.json`, the cargo releases, which represent the canonical
  version of bincode, are not fundamentally tied to the git repository at all,
  and there has been a lot of worrying about _the wrong supply chain_.

  We have faced many questions like "if the github is deleted, where can users
  make sure they are getting the old version of the source code" that are
  answered with "there haven't been any crates.io releases since the migration
  and history rewrite, just bring in an old version with cargo and you'll have
  it".

  This could have been potentially avoided if crates.io and cargo provided
  better visibility into their internal source control used for packing crates.
  At the very least, a link to the source code browser on docs.rs from the
  crates.io page for a given release could have made more users aware of the
  fact that crates.io/cargo provide such internal source control, and a full on
  source code viewer, or even better a version comparison tool, integrated into
  crates.io would have made things a _lot_ more manageable while there were
  still such questions flying around.

- crates.io metadata is not editable without performing a release

  Much of this could have been avoided if the crates.io metadata was
  independently editable and included a contact information field (ideally
  something like githubs private email solution, where it provides a generated
  email address that forwards to something configurable).

  At the time of migration, bincode was nowhere _near_ ready for another
  release, and being able to independently update the source code link in the
  crates.io metadata and provide up to date contact information without having
  to roll either a special no-code-changes release off of a branch that only
  changed those things, or roll a questionable intermediate release with the
  changes, both of which likely would have only raised more concerns from the
  community, would have significantly decreased the developer burden of
  completing the migration in a public and notorious fashion.