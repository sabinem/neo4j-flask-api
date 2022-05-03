import json
from flask import Response


def error_response(help, type, value, msg):
    return Response(json.dumps(
        {
            "help": help,
            "success": False,
            "error": {
                type: type,
                value: value,
                msg: msg,
            }
        }
    ), mimetype="application/json")