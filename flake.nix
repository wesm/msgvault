{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
  };

  outputs =
    { nixpkgs, ... }:
    let
      forAllSystems =
        fn:
        nixpkgs.lib.genAttrs [ "x86_64-linux" "aarch64-linux" "aarch64-darwin" "x86_64-darwin" ] (
          system: fn nixpkgs.legacyPackages.${system}
        );
    in
    {
      packages = forAllSystems (pkgs: {
        default =
          let
            # Pin Go 1.25.8 until nixpkgs-unstable catches up from staging
            go_pinned = pkgs.go_1_25.overrideAttrs (old: rec {
              version = "1.25.8";
              src = pkgs.fetchurl {
                url = "https://go.dev/dl/go${version}.src.tar.gz";
                hash = "sha256-6YjUokRqx/4/baoImljpk2pSo4E1Wt7ByJgyMKjWxZ4=";
              };
            });
          in
          (pkgs.buildGoModule.override { go = go_pinned; }) {
            pname = "msgvault";
            version = "0.11.0";
            src = ./.;
            vendorHash = "sha256-cax7EHBXCWCYmpaAtcRTh+TR6moVzVsNQiUBKvfwuGw=";
            proxyVendor = true;
            subPackages = [ "cmd/msgvault" ];
            tags = [ "fts5" ];
            ldflags = [
              "-X github.com/wesm/msgvault/cmd/msgvault/cmd.Version=nix-dev"
            ];
          };
      });

      devShells = forAllSystems (pkgs: {
        default = pkgs.mkShell {
          packages = with pkgs; [
            go
            golangci-lint
            gcc
          ];
        };
      });
    };
}
