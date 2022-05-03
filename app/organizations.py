import json
from flask import Flask, g, Response, request
from app import app
from queries import organizations as q_organizations
from .routes import get_db


@app.route("/organizations")
def get_organizations():
    db = get_db()
    organizations = db.read_transaction(q_organizations.get_organizations)
    list_organizations = []
    dict_organizations = {
        organization[0]: {
            'name': organization[0],
            'title': json.dumps({
                'de': organization[1],
                'fr': organization[2],
                'en': organization[3],
                'it': organization[4],
            }),
        }
        for organization in organizations
    }
    for organization in organizations:
        name = organization[0]
        has_parent = db.read_transaction(q_organizations.get_parent, request.args.get("organization", name))
        if not has_parent:
            sub_organizations = db.read_transaction(q_organizations.get_sub_organizations, request.args.get("organization", name))
            parent_organization_dict = dict_organizations[name]
            children = []
            for sub_organization in sub_organizations:
                children.append(dict_organizations[sub_organization[0]])
            parent_organization_dict['children'] = children
            list_organizations.append(
                parent_organization_dict
            )
    return Response(json.dumps(
        {
            "help": request.url,
            "success": True,
            "result": list_organizations
        }), mimetype="application/json")
