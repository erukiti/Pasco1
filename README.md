Pasco1
======

* オンラインで編集可能な文章共有システム
    * markdownフォーマット (Github Flavored Markdown)
    * リアルタイムプレビュー

必要なミドルウェア
------------------

* RiakCS or AmazonS3
* Redis

設定
----

```json
{
    "s3_key": "",
    "s3_secret": "",
    "s3_host": "localhost",
    "s3_port": "8080",
    "digest_algorithm": "SHA-256",
    "redis_host": "localhost"
}
```

* digest_algorithm は、文書などデータ保管に使うキーのハッシュアルゴリズム
    * たぶん SHA-1 で十分だとは思うけど、現状では SHA-256 のみ対応

マイルストーン
--------------

1. CLIツールとして、ユーザー情報、文書データなどの操作をできるようにする
2. webアプリケーションとして動くようにする

Won(\*3\*)Chu FixMe!
--------------------

PullReqなどお待ちしております。

ライセンス
----------

BSD style license.