# Captured consumer dependency declarations

These fixtures contain only the three shared-runtime dependency tables from
read-only references inspected on 2026-10-19. Tests never open a sibling checkout,
fetch a dependency, or import consumer crates.

- Open Forge `78702e7c4821d788ba05b7b484e80abc5c5c068e`, next/v0.14,
  `Cargo.toml:37`: exact `rev = "82f5aac"`; `Cargo.lock:24` resolves it to
  `82f5aac57a3b1012b4e96c1903f0ab4a0f00a625`. Its checkpoint DTO and validation
  have no diff from the runtime base of this change. This is existing immutable
  pinning, not evidence that Forge adopted this candidate.
- Nyx `585939d0d518e8a243c458016ed11f274d283f6d`, `Cargo.toml:76-80`:
  facade/core/testkit all use `branch = "main"`. `Cargo.lock:58` resolves to
  `22e41fa9a8e78f6ff0101af7743806a1d7867c5c`; that lock does not turn a moving
  manifest into immutable release adoption. The test requires the pin checker
  to reject this fixture. **Nyx release eligibility remains NOT MET.**
- Smith: no source checkout or dependency manifest was supplied. Its existing
  neutral testkit fixtures are the sole evidence; no Smith pin is invented.

Synthetic matching tag/revision cases test the adoption rule. They do not
attest publication, tag immutability, or coordinated adoption in any product.
