import sqlite3

import pandas as pd

from biz.entity.review_entity import MergeRequestReviewEntity, PushReviewEntity


class ReviewService:
    DB_FILE = "data/data.db"

    @staticmethod
    def init_db():
        """初始化数据库及表结构"""
        try:
            with sqlite3.connect(ReviewService.DB_FILE) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                        CREATE TABLE IF NOT EXISTS mr_review_log (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            project_name TEXT,
                            author TEXT,
                            source_branch TEXT,
                            target_branch TEXT,
                            updated_at INTEGER,
                            commit_messages TEXT,
                            score INTEGER,
                            url TEXT,
                            review_result TEXT,
                            additions INTEGER DEFAULT 0,
                            deletions INTEGER DEFAULT 0,
                            gitlab_group TEXT DEFAILT ''
                        )
                    ''')
                cursor.execute('''
                        CREATE TABLE IF NOT EXISTS push_review_log (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            project_name TEXT,
                            author TEXT,
                            branch TEXT,
                            updated_at INTEGER,
                            commit_messages TEXT,
                            score INTEGER,
                            review_result TEXT,
                            additions INTEGER DEFAULT 0,
                            deletions INTEGER DEFAULT 0,
                            gitlab_group TEXT DEFAILT ''
                        )
                    ''')
                # 确保旧版本的mr_review_log、push_review_log表添加additions、deletions、gitlab_group列
                tables = ["mr_review_log", "push_review_log"]
                columns = [
                    {
                        "name": "additions",
                        "type": "INTEGER",
                        "default": "0"
                    },
                    {
                        "name": "deletions",
                        "type": "INTEGER",
                        "default": "0"
                    },
                    {
                        "name": "gitlab_group",
                        "type": "TEXT",
                        "default": "''"
                    }
                ]
                for table in tables:
                    cursor.execute(f"PRAGMA table_info({table})")
                    current_columns = [col[1] for col in cursor.fetchall()]
                    for column in columns:
                        if column.get("name") not in current_columns:
                            cursor.execute(f"ALTER TABLE {table} ADD COLUMN {column.get('name')} {column.get('type')} "
                                           f"DEFAULT {column.get('default')}")
                conn.commit()
        except sqlite3.DatabaseError as e:
            print(f"Database initialization failed: {e}")

    @staticmethod
    def insert_mr_review_log(entity: MergeRequestReviewEntity):
        """插入合并请求审核日志"""
        try:
            with sqlite3.connect(ReviewService.DB_FILE) as conn:
                cursor = conn.cursor()
                cursor.execute('''INSERT INTO mr_review_log (gitlab_group, project_name, author, source_branch, 
                target_branch, updated_at, commit_messages, score, url,review_result, additions, deletions)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                               (entity.gitlab_group, entity.project_name, entity.author,
                                entity.source_branch, entity.target_branch,
                                entity.updated_at, entity.commit_messages, entity.score,
                                entity.url, entity.review_result, entity.additions, entity.deletions))
                conn.commit()
        except sqlite3.DatabaseError as e:
            print(f"Error inserting review log: {e}")

    @staticmethod
    def get_mr_review_logs(gitlab_groups: list = None, authors: list = None, project_names: list = None,
                           updated_at_gte: int = None, updated_at_lte: int = None) -> pd.DataFrame:
        """获取符合条件的合并请求审核日志"""
        try:
            with sqlite3.connect(ReviewService.DB_FILE) as conn:
                query = """
                            SELECT gitlab_group, project_name, author, source_branch, target_branch, updated_at, 
                            commit_messages, score, url, review_result, additions, deletions
                            FROM mr_review_log
                            WHERE 1=1
                            """
                params = []

                if gitlab_groups:
                    placeholders = ','.join(['?'] * len(gitlab_groups))
                    query += f" AND gitlab_group IN ({placeholders})"
                    params.extend(gitlab_groups)

                if authors:
                    placeholders = ','.join(['?'] * len(authors))
                    query += f" AND author IN ({placeholders})"
                    params.extend(authors)

                if project_names:
                    placeholders = ','.join(['?'] * len(project_names))
                    query += f" AND project_name IN ({placeholders})"
                    params.extend(project_names)

                if updated_at_gte is not None:
                    query += " AND updated_at >= ?"
                    params.append(updated_at_gte)

                if updated_at_lte is not None:
                    query += " AND updated_at <= ?"
                    params.append(updated_at_lte)
                query += " ORDER BY updated_at DESC"
                df = pd.read_sql_query(sql=query, con=conn, params=params)
            return df
        except sqlite3.DatabaseError as e:
            print(f"Error retrieving review logs: {e}")
            return pd.DataFrame()

    @staticmethod
    def insert_push_review_log(entity: PushReviewEntity):
        """插入推送审核日志"""
        try:
            with sqlite3.connect(ReviewService.DB_FILE) as conn:
                cursor = conn.cursor()
                cursor.execute('''INSERT INTO push_review_log (gitlab_group, project_name, author, branch, 
                updated_at, commit_messages, score, review_result, additions, deletions)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                               (entity.gitlab_group, entity.project_name, entity.author, entity.branch,
                                entity.updated_at, entity.commit_messages, entity.score,
                                entity.review_result, entity.additions, entity.deletions))
                conn.commit()
        except sqlite3.DatabaseError as e:
            print(f"Error inserting review log: {e}")

    @staticmethod
    def get_push_review_logs(gitlab_groups: list = None, authors: list = None, project_names: list = None,
                             updated_at_gte: int = None, updated_at_lte: int = None) -> pd.DataFrame:
        """获取符合条件的推送审核日志"""
        try:
            with sqlite3.connect(ReviewService.DB_FILE) as conn:
                # 基础查询
                query = """
                    SELECT gitlab_group, project_name, author, branch, updated_at, commit_messages, score, 
                    review_result, additions, deletions
                    FROM push_review_log
                    WHERE 1=1
                """
                params = []

                if gitlab_groups:
                    placeholders = ','.join(['?'] * len(gitlab_groups))
                    query += f" AND gitlab_group IN ({placeholders})"
                    params.extend(gitlab_groups)

                # 动态添加 authors 条件
                if authors:
                    placeholders = ','.join(['?'] * len(authors))
                    query += f" AND author IN ({placeholders})"
                    params.extend(authors)

                if project_names:
                    placeholders = ','.join(['?'] * len(project_names))
                    query += f" AND project_name IN ({placeholders})"
                    params.extend(project_names)

                # 动态添加 updated_at_gte 条件
                if updated_at_gte is not None:
                    query += " AND updated_at >= ?"
                    params.append(updated_at_gte)

                # 动态添加 updated_at_lte 条件
                if updated_at_lte is not None:
                    query += " AND updated_at <= ?"
                    params.append(updated_at_lte)

                # 按 updated_at 降序排序
                query += " ORDER BY updated_at DESC"

                # 执行查询
                df = pd.read_sql_query(sql=query, con=conn, params=params)
                return df
        except sqlite3.DatabaseError as e:
            print(f"Error retrieving push review logs: {e}")
            return pd.DataFrame()


# Initialize database
ReviewService.init_db()
