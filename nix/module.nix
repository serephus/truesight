{
  config,
  lib,
  pkgs,
  ...
}:
let
  cfg = config.services.truesight;
  bool = lib.boolToString;
  clickhouseManaged = cfg.clickhouse.enable && config.services.clickhouse.enable;
in
{
  options.services.truesight = {
    enable = lib.mkEnableOption "Truesight Dota 2 hero rating service";

    package = lib.mkOption {
      type = lib.types.package;
      default = pkgs.callPackage ./package.nix { };
      defaultText = lib.literalExpression "pkgs.callPackage ./package.nix { }";
      description = "Truesight package to run.";
    };

    user = lib.mkOption {
      type = lib.types.str;
      default = "truesight";
      description = "User that runs the service.";
    };

    group = lib.mkOption {
      type = lib.types.str;
      default = "truesight";
      description = "Group that runs the service.";
    };

    bind = lib.mkOption {
      type = lib.types.str;
      default = "127.0.0.1";
      description = "Address the HTTP server binds to.";
    };

    port = lib.mkOption {
      type = lib.types.port;
      default = 8080;
      description = "Port the HTTP server listens on.";
    };

    openFirewall = lib.mkOption {
      type = lib.types.bool;
      default = false;
      description = "Open `port` in the firewall. Set `bind` to 0.0.0.0 for direct access.";
    };

    logLevel = lib.mkOption {
      type = lib.types.str;
      default = "info,truesight=debug";
      description = "Tracing filter (`RUST_LOG` syntax).";
    };

    steamApiKeysFile = lib.mkOption {
      type = lib.types.nullOr lib.types.path;
      default = null;
      description = ''
        File containing one or more Steam Web API keys, separated by commas or
        whitespace. Loaded through systemd credentials, so the secret does not
        appear in the world-readable unit file.
      '';
    };

    collector = {
      enable = lib.mkOption {
        type = lib.types.bool;
        default = true;
        description = "Tail the public ranked match stream.";
      };
      pollIntervalMs = lib.mkOption {
        type = lib.types.ints.positive;
        default = 2000;
        description = "Initial polling interval; the collector adapts between the min and max values.";
      };
      minPollIntervalMs = lib.mkOption {
        type = lib.types.ints.positive;
        default = 250;
        description = "Fastest polling interval when the collector is behind.";
      };
      maxPollIntervalMs = lib.mkOption {
        type = lib.types.ints.positive;
        default = 30000;
        description = "Slowest polling interval when caught up or backing off after errors.";
      };
      pollStepMs = lib.mkOption {
        type = lib.types.ints.positive;
        default = 500;
        description = "Additive step used when slowing down near the head.";
      };
      maxLagSeconds = lib.mkOption {
        type = lib.types.ints.unsigned;
        default = 3600;
        description = "Hard lag ceiling; above this the collector speeds up.";
      };
      minLagSeconds = lib.mkOption {
        type = lib.types.ints.unsigned;
        default = 300;
        description = "Caught-up lag floor; below this the collector slows down.";
      };
      lagTrendThresholdSeconds = lib.mkOption {
        type = lib.types.ints.unsigned;
        default = 30;
        description = "Lag change that triggers a trend-based interval adjustment.";
      };
      batchSize = lib.mkOption {
        type = lib.types.ints.between 1 100;
        default = 100;
        description = "Matches requested per Steam call (API maximum is 100).";
      };
      startSeq = lib.mkOption {
        type = lib.types.nullOr lib.types.int;
        default = null;
        description = "Sequence to start from on first boot. Defaults to the newest public match.";
      };
    };

    ranker = {
      enable = lib.mkOption {
        type = lib.types.bool;
        default = true;
        description = "Periodically fit day/week/month ratings.";
      };
      interval = lib.mkOption {
        type = lib.types.ints.positive;
        default = 300;
        description = "Seconds between rating fits.";
      };
      lambda = lib.mkOption {
        type = lib.types.float;
        default = 1.0;
        description = "L2 regularization strength for Bradley–Terry ratings.";
      };
      minGames = lib.mkOption {
        type = lib.types.ints.unsigned;
        default = 1;
        description = "Minimum games in a window before a hero is published.";
      };
    };

    patch = {
      name = lib.mkOption {
        type = lib.types.str;
        default = "current";
        description = "Patch label stored with rating snapshots.";
      };
      start = lib.mkOption {
        type = lib.types.nullOr lib.types.int;
        default = null;
        description = ''
          Unix timestamp at which the current patch started. When set, every
          window is clipped to this timestamp so ratings never mix patches.
        '';
      };
    };

    clickhouse = {
      enable = lib.mkOption {
        type = lib.types.bool;
        default = true;
        description = "Run a local ClickHouse server managed by NixOS.";
      };
      url = lib.mkOption {
        type = lib.types.str;
        default = "http://127.0.0.1:8123";
        description = "ClickHouse HTTP endpoint.";
      };
      database = lib.mkOption {
        type = lib.types.str;
        default = "truesight";
        description = "Database name.";
      };
      user = lib.mkOption {
        type = lib.types.str;
        default = "default";
        description = "ClickHouse user.";
      };
      passwordFile = lib.mkOption {
        type = lib.types.nullOr lib.types.path;
        default = null;
        description = "File containing the ClickHouse password, loaded as a systemd credential.";
      };
      serverConfig = lib.mkOption {
        type = lib.types.attrs;
        default = { };
        description = "Extra ClickHouse server configuration merged into the managed instance.";
      };
    };

    rawRetentionDays = lib.mkOption {
      type = lib.types.ints.unsigned;
      default = 0;
      description = ''
        Drop the raw match JSON column after this many days (structured columns
        and aggregates are kept). 0 keeps raw payloads forever.
      '';
    };

    environment = lib.mkOption {
      type = lib.types.attrsOf lib.types.str;
      default = { };
      description = "Extra environment variables for the service.";
    };

    nginx = {
      enable = lib.mkEnableOption "an nginx virtual host in front of Truesight";
      serverName = lib.mkOption {
        type = lib.types.str;
        default = "truesight.local";
        description = "nginx server name.";
      };
      enableACME = lib.mkOption {
        type = lib.types.bool;
        default = false;
        description = "Request a Let's Encrypt certificate.";
      };
      forceSSL = lib.mkOption {
        type = lib.types.bool;
        default = false;
        description = "Redirect HTTP to HTTPS.";
      };
    };
  };

  config = lib.mkIf cfg.enable {
    assertions = [
      {
        assertion =
          !cfg.collector.enable
          || cfg.steamApiKeysFile != null
          || cfg.environment ? TRUESIGHT_STEAM_API_KEYS
          || cfg.environment ? TRUESIGHT_STEAM_API_KEYS_FILE;
        message = ''
          services.truesight.steamApiKeysFile is required when the collector is
          enabled (or provide TRUESIGHT_STEAM_API_KEYS in environment).
        '';
      }
      {
        assertion = cfg.collector.minPollIntervalMs <= cfg.collector.maxPollIntervalMs;
        message = "services.truesight.collector.minPollIntervalMs must be <= maxPollIntervalMs";
      }
      {
        assertion = cfg.collector.minLagSeconds <= cfg.collector.maxLagSeconds;
        message = "services.truesight.collector.minLagSeconds must be <= maxLagSeconds";
      }
    ];

    users.users.${cfg.user} = {
      isSystemUser = true;
      group = cfg.group;
      description = "Truesight service user";
    };
    users.groups.${cfg.group} = { };

    services.clickhouse = lib.mkIf cfg.clickhouse.enable {
      enable = lib.mkDefault true;
      serverConfig = lib.mkMerge [
        {
          listen_host = lib.mkDefault "127.0.0.1";
          http_port = lib.mkDefault 8123;
          tcp_port = lib.mkDefault 9000;
          logger = lib.mkDefault {
            console = 1;
            level = "information";
          };
        }
        cfg.clickhouse.serverConfig
      ];
    };

    systemd.services.truesight = {
      description = "Truesight Dota 2 hero ratings";
      wantedBy = [ "multi-user.target" ];
      wants = [ "network-online.target" ] ++ lib.optional clickhouseManaged "clickhouse.service";
      after = [ "network-online.target" ] ++ lib.optional clickhouseManaged "clickhouse.service";
      requires = lib.optional clickhouseManaged "clickhouse.service";

      serviceConfig = {
        ExecStart = "${cfg.package}/bin/truesight";
        User = cfg.user;
        Group = cfg.group;
        Restart = "on-failure";
        RestartSec = 5;
        UMask = "0077";

        LoadCredential =
          lib.optional (cfg.steamApiKeysFile != null)
            "steam_api_keys:${toString cfg.steamApiKeysFile}"
          ++ lib.optional (cfg.clickhouse.passwordFile != null)
            "clickhouse_password:${toString cfg.clickhouse.passwordFile}";

        Environment =
          [
            "TRUESIGHT_BIND=${cfg.bind}:${toString cfg.port}"
            "TRUESIGHT_LOG=${cfg.logLevel}"
            "TRUESIGHT_CLICKHOUSE_URL=${cfg.clickhouse.url}"
            "TRUESIGHT_CLICKHOUSE_DATABASE=${cfg.clickhouse.database}"
            "TRUESIGHT_CLICKHOUSE_USER=${cfg.clickhouse.user}"
            "TRUESIGHT_COLLECTOR_ENABLED=${bool cfg.collector.enable}"
            "TRUESIGHT_POLL_INTERVAL_MS=${toString cfg.collector.pollIntervalMs}"
            "TRUESIGHT_MIN_POLL_INTERVAL_MS=${toString cfg.collector.minPollIntervalMs}"
            "TRUESIGHT_MAX_POLL_INTERVAL_MS=${toString cfg.collector.maxPollIntervalMs}"
            "TRUESIGHT_POLL_STEP_MS=${toString cfg.collector.pollStepMs}"
            "TRUESIGHT_MAX_LAG_SECS=${toString cfg.collector.maxLagSeconds}"
            "TRUESIGHT_MIN_LAG_SECS=${toString cfg.collector.minLagSeconds}"
            "TRUESIGHT_LAG_TREND_THRESHOLD_SECS=${toString cfg.collector.lagTrendThresholdSeconds}"
            "TRUESIGHT_BATCH_SIZE=${toString cfg.collector.batchSize}"
            "TRUESIGHT_RANKER_ENABLED=${bool cfg.ranker.enable}"
            "TRUESIGHT_RANK_INTERVAL_SECS=${toString cfg.ranker.interval}"
            "TRUESIGHT_REGULARIZATION=${toString cfg.ranker.lambda}"
            "TRUESIGHT_MIN_RATED_GAMES=${toString cfg.ranker.minGames}"
            "TRUESIGHT_RAW_RETENTION_DAYS=${toString cfg.rawRetentionDays}"
          ]
          ++ lib.optional (cfg.collector.startSeq != null)
            "TRUESIGHT_START_SEQ=${toString cfg.collector.startSeq}"
          ++ lib.optional (cfg.patch.start != null)
            "TRUESIGHT_PATCH_START=${toString cfg.patch.start}"
          ++ lib.optional (cfg.patch.name != "")
            "TRUESIGHT_PATCH_NAME=${cfg.patch.name}"
          ++ lib.mapAttrsToList (name: value: "${name}=${value}") cfg.environment;

        NoNewPrivileges = true;
        PrivateDevices = true;
        PrivateTmp = true;
        ProtectClock = true;
        ProtectControlGroups = true;
        ProtectHome = true;
        ProtectKernelLogs = true;
        ProtectKernelModules = true;
        ProtectKernelTunables = true;
        ProtectSystem = "strict";
        RestrictAddressFamilies = [
          "AF_INET"
          "AF_INET6"
          "AF_NETLINK"
          "AF_UNIX"
        ];
        RestrictNamespaces = true;
        RestrictRealtime = true;
        LockPersonality = true;
        MemoryDenyWriteExecute = true;
        RemoveIPC = true;
        SystemCallArchitectures = "native";
      };
    };

    networking.firewall.allowedTCPPorts =
      lib.optional cfg.openFirewall cfg.port ++ lib.optionals cfg.nginx.enable [ 80 443 ];

    services.nginx = lib.mkIf cfg.nginx.enable {
      enable = true;
      virtualHosts.${cfg.nginx.serverName} = {
        inherit (cfg.nginx) enableACME forceSSL;
        locations."/" = {
          proxyPass = "http://127.0.0.1:${toString cfg.port}";
          proxyWebsockets = true;
        };
      };
    };
  };
}
