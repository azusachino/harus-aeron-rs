{
  description = "Aeron protocol implementation in Rust";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils, ... }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
      in
      {
        devShells.default = pkgs.mkShell {
          packages = with pkgs; [
            rustc
            cargo
            clippy
            rustfmt
            rust-analyzer
            prettier
            uv # orchestration scripts (scripts/*.py, PEP 723 inline deps)
            podman-compose # local CI interop smoke, once deploy/interop exists
          ];

          shellHook = ''
            export IN_NIX_SHELL=1
          '';
        };
      });
}
