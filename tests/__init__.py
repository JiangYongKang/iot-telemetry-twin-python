"""测试包：统一打开判定日志，保证 `python -m unittest discover` 时
也能打印每次判定的“输入与依据”，无需额外 pytest 插件。"""

import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s | %(message)s",
)
