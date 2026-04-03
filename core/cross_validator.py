"""
多源交叉验证引擎
核心原则：每个数据点至少2个独立来源确认，误差>5%需人工复核
"""

import re
from dataclasses import dataclass, field
from typing import Optional
from config.settings import PE_CROSS_TOLERANCE, MIN_SOURCES_FOR_CONFIRMATION
from utils.logger import setup_logger

logger = setup_logger("cross_validator")


@dataclass
class DataPoint:
    """单个数据点（来自某一数据源）"""
    value: float
    source: str
    confidence: float = 1.0


@dataclass
class CrossValidationResult:
    """交叉验证结果"""
    field: str                        # 数据字段名（pe/price/market_cap）
    confirmed: bool                   # 是否通过验证（≥2源确认）
    consensus_value: Optional[float]  # 共识值（取均值）
    sources: list = field(default_factory=list)
    max_deviation: float = 0.0
    needs_review: bool = False        # 误差>5%需人工复核
    note: str = ""


class CrossValidator:
    """
    多源交叉验证器
    规则：
    1. 至少2个独立来源确认
    2. 误差 > PE_CROSS_TOLERANCE (5%) → 标记人工复核
    3. 只有1个来源 → 确认失败（数据点无效）
    """

    def validate_pe(self, data_points: list[DataPoint]) -> CrossValidationResult:
        return self._validate("pe", data_points)

    def validate_price(self, data_points: list[DataPoint]) -> CrossValidationResult:
        return self._validate("price", data_points, tolerance=0.02)

    def validate_market_cap(self, data_points: list[DataPoint]) -> CrossValidationResult:
        return self._validate("market_cap", data_points, tolerance=0.03)

    def _validate(self, field: str, points: list[DataPoint], tolerance: float = PE_CROSS_TOLERANCE) -> CrossValidationResult:
        # 过滤掉None值
        valid = [p for p in points if p.value is not None and p.value > 0]

        if len(valid) < MIN_SOURCES_FOR_CONFIRMATION:
            logger.warning(f"[验证] {field}: 仅{len(valid)}个有效来源，不足{MIN_SOURCES_FOR_CONFIRMATION}个，验证失败")
            return CrossValidationResult(
                field=field,
                confirmed=False,
                consensus_value=None,
                sources=[p.source for p in valid],
                note=f"来源不足（需≥{MIN_SOURCES_FOR_CONFIRMATION}，实际{len(valid)}）"
            )

        values = [p.value for p in valid]
        mean_val = sum(values) / len(values)
        max_dev = max(abs(v - mean_val) / mean_val for v in values) if mean_val != 0 else 0

        needs_review = max_dev > tolerance

        if needs_review:
            logger.warning(
                f"[验证] {field}: 最大偏差{max_dev:.1%} > {tolerance:.0%}，"
                f"需人工复核。来源: {[(p.source, p.value) for p in valid]}"
            )
        else:
            logger.info(
                f"[验证] {field}: ✓ 交叉验证通过 | 共识值={mean_val:.2f} | "
                f"偏差={max_dev:.1%} | 来源={[p.source for p in valid]}"
            )

        return CrossValidationResult(
            field=field,
            confirmed=True,
            consensus_value=round(mean_val, 4),
            sources=[f"{p.source}:{p.value}" for p in valid],
            max_deviation=round(max_dev, 4),
            needs_review=needs_review,
            note="人工复核：偏差过大" if needs_review else "✓ 交叉验证通过"
        )


class IntegrityChecker:
    """
    数据完整性自检
    核心原则：股价/PE/市值/近期事件 四项全有才算通过，缺一不可
    """

    REQUIRED_FIELDS = {
        "price":         "股价",
        "pe":            "市盈率(PE)",
        "market_cap":    "总市值",
        "recent_events": "近期事件",
    }

    def check(self, stock_data: dict) -> tuple[bool, list[str], list[str]]:
        """
        返回: (是否通过, 通过的字段列表, 缺失的字段列表)
        """
        passed = []
        missing = []

        for field, label in self.REQUIRED_FIELDS.items():
            val = stock_data.get(field)

            has_value = False
            if val is None:
                has_value = False
            elif isinstance(val, (int, float)):
                has_value = val > 0
            elif isinstance(val, str):
                has_value = bool(val.strip()) and val not in ["—", "无", "null", "None"]
            elif isinstance(val, list):
                has_value = len(val) > 0
            elif isinstance(val, dict):
                has_value = bool(val)

            if has_value:
                passed.append(label)
            else:
                missing.append(label)
                logger.warning(f"[完整性] 缺失字段: {label} ({field})")

        all_pass = len(missing) == 0

        if all_pass:
            logger.info(f"[完整性] ✓ 全部{len(passed)}项通过: {passed}")
        else:
            logger.error(f"[完整性] ✗ 缺失{len(missing)}项: {missing} → 直接排除，不进入决策")

        return all_pass, passed, missing


def extract_pe_from_text(text: str, stock_name: str) -> Optional[float]:
    """
    从搜索结果文本中提取PE值
    支持格式：20.40、20.40倍、PE:20.40、市盈率20.40
    """
    if not text:
        return None

    patterns = [
        rf"{re.escape(stock_name)}.*?(?:PE|市盈率|市盈率TTM|动态PE)[：:=\s]*([0-9]+\.?[0-9]*)",
        r"(?:PE|市盈率|市盈率TTM|动态PE)[：:=\s]*([0-9]+\.?[0-9]*)",
        r"([0-9]+\.[0-9]+)\s*(?:倍|x|X)",
    ]

    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
        if match:
            try:
                val = float(match.group(1))
                # PE合理范围过滤（A股一般0~500）
                if 0 < val < 500:
                    return val
            except ValueError:
                continue
    return None


def extract_market_cap_from_text(text: str) -> Optional[float]:
    """
    从文本中提取市值（单位：亿元）
    支持：2100亿、2.1万亿、2100亿元
    """
    if not text:
        return None

    # 万亿
    m = re.search(r"([0-9]+\.?[0-9]*)\s*万亿", text)
    if m:
        return float(m.group(1)) * 10000

    # 亿
    m = re.search(r"([0-9]+\.?[0-9]*)\s*亿", text)
    if m:
        val = float(m.group(1))
        if val > 1:  # 过滤过小值
            return val

    return None
