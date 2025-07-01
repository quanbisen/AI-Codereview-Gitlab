import json
import os

from biz.llm.factory import Factory
from biz.utils.token_util import count_tokens
from biz.utils.log import logger


class Reporter:
    def __init__(self):
        self.client = Factory().getClient()

    def generate_report(self, data: str) -> str:
        # 如果超长，取前REVIEW_MAX_TOKENS个token
        review_max_tokens = int(os.getenv("REVIEW_MAX_TOKENS", 10000))
        # 计算tokens数量，如果超过REVIEW_MAX_TOKENS，去除review_result
        tokens_count = count_tokens(data)
        if tokens_count > review_max_tokens:
            logger.info(f"数据长度超过{review_max_tokens}，进行去除review_result字段")
            json_data = json.loads(data)
            for item in json_data:
                item.pop('review_result', None)  # 删除review_result字段，如果字段不存在不会报错
            data = json.dumps(json_data)
        # 根据data生成报告
        return self.client.completions(
            messages=[
                {"role": "user", "content": f"下面是以json格式记录员工代码提交信息。请总结这些信息，生成每个员工的工作日报摘要。员工姓名直接用json内容中的author属性值，不要进行转换。特别要求:以Markdown格式返回。\n{data}"},
            ],
        )
