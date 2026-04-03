#!/usr/bin/env python3
"""
Web看板启动脚本
用法:
  python start_web.py           # 默认 http://localhost:8000
  python start_web.py --port 9000
  python start_web.py --reload  # 开发模式（代码改动自动重启）
"""
import sys
import subprocess

def main():
    port   = "8000"
    reload = False
    for i, arg in enumerate(sys.argv[1:]):
        if arg == "--port" and i+2 <= len(sys.argv)-1:
            port = sys.argv[i+2]
        if arg == "--reload":
            reload = True

    cmd = [
        sys.executable, "-m", "uvicorn",
        "web.app:app",
        "--host", "0.0.0.0",
        "--port", port,
    ]
    if reload:
        cmd.append("--reload")

    print(f"\n{'='*50}")
    print(f"  A股量化交易系统 Web看板")
    print(f"  浏览器访问: http://localhost:{port}")
    print(f"  API文档:    http://localhost:{port}/docs")
    print(f"{'='*50}\n")
    subprocess.run(cmd)

if __name__ == "__main__":
    main()
