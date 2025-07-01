import base64
import hashlib
import hmac
import json
import os
import time
import urllib.parse

import requests

from biz.utils.log import logger


class DingTalkNotifier:
    def __init__(self, webhook_url=None):
        self.enabled = os.environ.get('DINGTALK_ENABLED', '0') == '1'
        self.default_webhook_url = webhook_url or os.environ.get('DINGTALK_WEBHOOK_URL')

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
        
        # 如果未提供 project_name、url_slug 和 gitlab_group，直接返回默认的 Webhook URL
        if not project_name and not url_slug and not gitlab_group:
            if fallback_url:
                return fallback_url
            raise ValueError("未提供项目名称、url_slug、gitlab组，且未设置默认的钉钉 Webhook URL。")

        # 构建环境变量键名和优先级列表
        env_keys = []
        if project_name:
            env_keys.append((f"DINGTALK_WEBHOOK_URL_{project_name.upper()}", f"项目 '{project_name}'"))
        if url_slug:
            env_keys.append((f"DINGTALK_WEBHOOK_URL_{url_slug.upper()}", f"URL Slug '{url_slug}'"))
        if gitlab_group:
            env_keys.append((f"DINGTALK_WEBHOOK_URL_{gitlab_group.upper()}", f"GitLab Group '{gitlab_group}'"))
        
        # 按优先级检查环境变量
        for key, _ in env_keys:
            if key in os.environ:
                return os.environ[key]
        
        # 如果未找到匹配的环境变量，降级使用全局的 Webhook URL
        if fallback_url:
            return fallback_url
            
        # 构建错误消息，包含所有尝试的键
        error_sources = " 或 ".join([desc for _, desc in env_keys])
        raise ValueError(f"未找到{error_sources}对应的钉钉 Webhook URL，且未设置默认的 Webhook URL。")

    def send_message(self, content: str, msg_type='text', title='通知', is_at_all=False, project_name=None,
                     url_slug=None, gitlab_group=None):
        if not self.enabled:
            logger.info("钉钉推送未启用")
            return

        try:
            post_url = self._get_webhook_url(project_name=project_name, url_slug=url_slug, gitlab_group=gitlab_group)
            headers = {
                "Content-Type": "application/json",
                "Charset": "UTF-8"
            }
            if msg_type == 'markdown':
                message = {
                    "msgtype": "markdown",
                    "markdown": {
                        "title": title,  # Customize as needed
                        "text": content
                    },
                    "at": {
                        "isAtAll": is_at_all
                    }
                }
            else:
                message = {
                    "msgtype": "text",
                    "text": {
                        "content": content
                    },
                    "at": {
                        "isAtAll": is_at_all
                    }
                }
            response = requests.post(url=post_url, data=json.dumps(message), headers=headers)
            response_data = response.json()
            if response_data.get('errmsg') == 'ok':
                logger.info(f"钉钉消息发送成功! webhook_url:{post_url}")
            else:
                logger.error(f"钉钉消息发送失败! webhook_url:{post_url},errmsg:{response_data.get('errmsg')}")
        except Exception as e:
            logger.error(f"钉钉消息发送失败! ", e)
