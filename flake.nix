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
            # Pin Go 1.25.7 until nixpkgs-unstable catches up from staging
            go_pinned = pkgs.go_1_25.overrideAttrs (old: rec {
              version = "1.25.7";
              src = pkgs.fetchurl {
                url = "https://go.dev/dl/go${version}.src.tar.gz";
                hash = "sha256-F48oMoICdLQ+F30y8Go+uwEp5CfdIKXkyI3ywXY88Qo=";
              };
            });
          in
          (pkgs.buildGoModule.override { go = go_pinned; }) {
            pname = "msgvault";
            version = "0.0.0-dev";
            src = ./.;
            vendorHash = "sha256-rBH+otJye6ocIaOcnI1g9cFtoGzs+ay6H9zfQgNGyi8=";
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
