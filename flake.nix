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
        default = pkgs.buildGoModule {
          pname = "msgvault";
          version = "0.0.0-dev";
          src = ./.;
          vendorHash = "sha256-oHaCUxbrCNjw31rLrbYuwarU+nEX84t9RgH7Ubw/b9s=";
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
            go_1_25
            golangci-lint
            gcc
          ];
        };
      });
    };
}
