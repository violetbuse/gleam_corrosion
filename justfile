run-corrosion:
  corrosion agent --config ./corrosion.toml

reset-corrosion:
  rm -rf ./corrosion.db ./corrosion.db-shm ./corrosion.db-wal ./corrosion.admin.sock
