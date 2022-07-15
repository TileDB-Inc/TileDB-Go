# Subroutine to set up environment variables.

CORE_ROOT="$HOME/tiledb-core"

case "$(uname -s)" in
  Linux)
    OS="linux"
  ;;
  Darwin)
    OS="macos"
  ;;
  *)
    echo 'Unknown OS!'
    exit 1
  ;;
esac
