from flask import Flask, request, jsonify
import sys
sys.path.append("/usr/local/lib/python2.7/dist-packages/")
from flask_mail import Message, Mail
import logging
import requests
import json
import os

app = Flask(__name__)
mail = Mail(app)

def get_env(var):
    envvar = None
    if var.upper() in os.environ:
        envvar = os.environ[var.upper()]
    return envvar

base_url = get_env('BASE_URL')
app.config['MAIL_SERVER']='smtp.bouvet.no'
app.config['MAIL_PORT'] = 25
app.config['MAIL_USE_TLS'] = False
mail = Mail(app)

logger = logging.getLogger('Bouvet-smtp')
format_string = '%(asctime)s - %(lineno)d - %(levelname)s - %(message)s'
stdout_handler = logging.StreamHandler()
stdout_handler.setFormatter(logging.Formatter(format_string))
logger.addHandler(stdout_handler)
logger.setLevel(get_env('LOG_LEVEL'))

def mass_email(pipe, num, reason, mail_header):
    msg = Message("SESAM " + mail_header, sender = "dont-reply@sesam.io", recipients = [get_env('MAIL_RECEIVER')])

    if reason == "dead-letters":
        msg.body = 'The integration {} failed for {} entities during the last hour \n For more information, please contact support@sesam.io or your direct Sesam contact.'.format(pipe, num)

    elif reason == "currentdepid":
        msg.body = "{} ad-users are managers and are missing CurrentDepartmentID \n For more information, please contact support@sesam.io or your direct Sesam contact.".format(num)

    else:
        logger.error("Missing reason statement from Sesam!")

    return msg
    

def find_key_string(dictionary):
    string = ""
    for i, key in enumerate(dictionary.keys()):
        try:
            if len(dictionary[key].keys()) != 0:
                string +="\n" + key + ": " + "{" + find_key_string(dictionary[key]) + "}"  
            else:
                string +="\n" + key + ": " + dictionary[key]
        except AttributeError:
            string += "\n" + key + ": " + str(dictionary[key])
        if i != len(dictionary.keys())-1:
            string += "," 
    return string


def individual_emails(entity, pipe, reason, mail_header):

    if reason == 'dead-letters':
        msg = Message("SESAM " + mail_header, sender = "dont-reply@sesam.io", recipients = [get_env('MAIL_RECEIVER')])
        payload = find_key_string(entity['entity']['payload'])
        msg.body = "The pipe %s failed at %s for entity %s \n\nOriginal error message: \n\n%s \nEntity body: %s \n\nFor more information, please contact support@sesam.io or your direct Sesam contact." %(entity['pipe'], entity['event_time'], entity['_id'], entity['original_error_message'], payload)
        #msg.body = "The pipe %s failed at %s for entity %s \n \n Original error message: \n %s \n Entity body: \n { \n     country_id: %s \n     email: %s \n     ensure_unique_custom_tag_ids_by_category: %s \n     external_unique_id: %s \n     name: %s \n     office_id: %s \n     role: %s \n     telephone: %s \n \n For more information, please contact support@sesam.io or your direct Sesam contact.}" %(entity['pipe'], entity['event_time'], entity['_id'], entity['original_error_message'], entity['entity']['payload']['user']['country_id'], entity['entity']['payload']['user']['email'], entity['entity']['payload']['user']['ensure_unique_custom_tag_ids_by_category'][list(entity['entity']['payload']['user']['ensure_unique_custom_tag_ids_by_category'].keys())[0]], entity['entity']['payload']['user']['external_unique_id'], entity['entity']['payload']['user']['name'], entity['entity']['payload']['user']['office_id'], entity['entity']['payload']['user']['role'], entity['entity']['payload']['user']['telephone'])
    elif reason == 'currentdepid':
        msg = Message("SESAM" + mail_header, sender = "dont-reply@sesam.io", recipients = [get_env('MAIL_RECEIVER')])
        msg.body = "AD-user %s is a manager but has no CurrentDepartmentID \n For more information, please contact support@sesam.io or your direct Sesam contact." % entity["employeeID"][0]
    logger.debug(string(msg))
    return msg

@app.route('/<string:pipe>/<string:reason>/<string:mail_header>', methods=['GET','POST'])
def main_func(pipe, reason, mail_header):
    entities = request.get_json()

    if len(entities) == 0:
        return "Done"
    if len(entities) > int(get_env('AMOUNT_CAP')):
        msg = mass_email(pipe, len(entities), reason, mail_header)
        delete_entities(msg, entities, pipe)
    else:
        for entity in entities:
            msg =  individual_emails(entity, pipe, reason, mail_header)
            delete_entities(msg, [entity], pipe)
    return "Done"

@app.route('/', methods=['GET','POST'])
def delete_entities(msg, entities, pipe):
    header = {'Authorization': "Bearer {}".format(get_env('SESAM_JWT')), "content_type": "application/json"}
    for entity in entities:
        entity["_deleted"] = True
        try:
            entity['_id'] = entity['type'][0][2:] + ':' + entity['_id']
        except KeyError:
            pass
        try:
            del entity['thumbnailPhoto']
        except KeyError:
            pass
        resp = requests.post(base_url + "datasets/%s/entities" % pipe, headers=header, data=json.dumps(entity), verify=False)
        if resp.status_code != 200:
            logger.error("Error in post to Sesam: status_code = {} for _id: {}".format(resp.status_code, entity['_id']))
        else:
            try:
                logger.send(msg)
                mail.send(msg)
                logger.info("Mail sent")
            except Exception as e:
                logger.error("Error during email-constuction: {}".format(e))

    
if __name__ == '__main__':

    # This is used when running locally. Gunicorn is used to run the
    # application on Google App Engine. See entrypoint in app.yaml.
    app.run(host='0.0.0.0', port=5000, debug=True, threaded=True)