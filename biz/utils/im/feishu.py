import requests
import os
from biz.utils.log import logger


class FeishuNotifier:
    def __init__(self, webhook_url=None):
        """
        初始化飞书通知器
        :param webhook_url: 飞书机器人webhook地址
        """
        self.default_webhook_url = webhook_url or os.environ.get('FEISHU_WEBHOOK_URL', '')
        self.enabled = os.environ.get('FEISHU_ENABLED', '0') == '1'

    def _get_webhook_url(self, project_name=None, url_slug=None, gitlab_group=None):
        """
        获取项目对应的 Webhook URL
        :param project_name: 项目名称
        :param url_slug: GitLab URL Slug
        :param gitlab_group: GitLab Group
        :return: Webhook URL
        :raises ValueError: 如果未找到 Webhook URL
        """
        # 如果有默认webhook_url，将其设为备选方案
        fallback_url = self.default_webhook_url
        
        # 如果未提供 project_name 和 gitlab_group，直接返回默认的 Webhook URL
        if not project_name and not url_slug and not gitlab_group:
            if fallback_url:
                return fallback_url
            raise ValueError("未提供项目名称、url_slug、gitlab组，且未设置默认的 飞书 Webhook URL。")

        # 构建环境变量键名和优先级列表
        env_keys = []
        if project_name:
            env_keys.append((f"FEISHU_WEBHOOK_URL_{project_name.upper()}", f"项目 '{project_name}'"))
        if url_slug:
            env_keys.append((f"FEISHU_WEBHOOK_URL_{url_slug.upper()}", f"URL Slug '{url_slug}'"))
        if gitlab_group:
            env_keys.append((f"FEISHU_WEBHOOK_URL_{gitlab_group.upper()}", f"GitLab Group '{gitlab_group}'"))
        
        # 按优先级检查环境变量
        for key, _ in env_keys:
            if key in os.environ:
                return os.environ[key]
        
        # 如果未找到匹配的环境变量，降级使用全局的 Webhook URL
        if fallback_url:
            return fallback_url
            
        # 构建错误消息，包含所有尝试的键
        error_sources = " 或 ".join([desc for _, desc in env_keys])
        raise ValueError(f"未找到{error_sources}对应的 Feishu Webhook URL，且未设置默认的 Webhook URL。")

    def send_message(self, content, msg_type='text', title=None, is_at_all=False, project_name=None, url_slug=None,
                     gitlab_group=None):
        """
        发送飞书消息
        :param content: 消息内容
        :param msg_type: 消息类型，支持text和markdown
        :param title: 消息标题(markdown类型时使用)
        :param is_at_all: 是否@所有人
        :param project_name: 项目名称
        :param url_slug: URL slug
        :param gitlab_group: GitLab group
        """
        if not self.enabled:
            logger.info("飞书推送未启用")
            return

        try:
            post_url = self._get_webhook_url(project_name=project_name, url_slug=url_slug, gitlab_group=gitlab_group)
            if msg_type == 'markdown':
                data = {
                    "msg_type": "interactive",
                    "card": {
                        "schema": "2.0",
                        "config": {
                            "update_multi": True,
                            "style": {
                                "text_size": {
                                    "normal_v2": {
                                        "default": "normal",
                                        "pc": "normal",
                                        "mobile": "heading"
                                    }
                                }
                            }
                        },
                        "body": {
                            "direction": "vertical",
                            "padding": "12px 12px 12px 12px",
                            "elements": [
                                {
                                    "tag": "markdown",
                                    "content": content,
                                    "text_align": "left",
                                    "text_size": "normal_v2",
                                    "margin": "0px 0px 0px 0px"
                                }
                            ]
                        },
                        "header": {
                            "title": {
                                "tag": "plain_text",
                                "content": title
                            },
                            "template": "blue",
                            "padding": "12px 12px 12px 12px"
                        }
                    }
                }
            else:
                data = {
                    "msg_type": "text",
                    "content": {
                        "text": content
                    },
                }

            response = requests.post(
                url=post_url,
                json=data,
                headers={'Content-Type': 'application/json'}
            )

            if response.status_code != 200:
                logger.error(f"飞书消息发送失败! webhook_url:{post_url}, error_msg:{response.text}")
                return

            result = response.json()
            if result.get('msg') != "success":
                logger.error(f"发送飞书消息失败! webhook_url:{post_url},errmsg:{result}")
            else:
                logger.info(f"飞书消息发送成功! webhook_url:{post_url}")

        except Exception as e:
            logger.error(f"飞书消息发送失败! ", e)
