#!/bin/zsh
# NotebookLM 去水印 · 网页版
cd "$(dirname "$0")"

# 优先使用项目自己的 Python 3.12 环境：它与线上容器的运行代际一致，
# 不会受 macOS 自带 Python 或其他项目依赖影响。
if [[ -x ".venv/bin/python" ]]; then
  exec .venv/bin/python -u server.py "$@"
fi

exec python3 -u server.py "$@"
