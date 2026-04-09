#!/bin/bash
# 配置 crontab：每个工作日 15:30 自动更新 A 股行情数据
# 运行方式：bash scripts/setup_crontab.sh

PROJ="/Users/zhuzhu/Documents/quant_system"
PYTHON="$PROJ/venv/bin/python3"
LOG="$PROJ/logs/update.log"
JOB="30 15 * * 1-5 cd $PROJ && $PYTHON -m scripts.daily_data_update >> $LOG 2>&1"

# 检查是否已存在该任务
if crontab -l 2>/dev/null | grep -q "daily_data_update"; then
    echo "⚠️  crontab 中已存在 daily_data_update 任务，跳过（避免重复）"
    echo "当前 crontab："
    crontab -l | grep daily_data_update
else
    (crontab -l 2>/dev/null; echo "$JOB") | crontab -
    echo "✅ crontab 已添加："
    echo "   $JOB"
fi

echo ""
echo "查看所有 crontab 任务：crontab -l"
echo "手动立即运行更新：  cd $PROJ && $PYTHON -m scripts.daily_data_update"
echo "只更新快照（快速）：cd $PROJ && $PYTHON -m scripts.daily_data_update --skip-kline"
