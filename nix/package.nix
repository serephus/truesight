{
  lib,
  rustPlatform,
}:
rustPlatform.buildRustPackage {
  pname = "truesight";
  version = "0.1.0";

  src = lib.cleanSourceWith {
    src = ../.;
    filter =
      path: _type:
      let
        name = baseNameOf (toString path);
      in
      name != "target" && name != ".clickhouse" && name != ".direnv";
  };
  cargoLock.lockFile = ../Cargo.lock;

  meta = {
    description = "Dota 2 hero strength ratings from public ranked matches";
    homepage = "https://github.com/serephus/truesight";
    license = lib.licenses.mit;
    mainProgram = "truesight";
    platforms = lib.platforms.linux;
  };
}
