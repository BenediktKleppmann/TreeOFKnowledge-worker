from flask import Flask
import traceback



# print a nice greeting.
def say_hello(username = "World"):
    return '<p>Hello %s!</p>\n' % username


# EB looks for an 'application' callable by default.
application = Flask(__name__)

# # add a rule for the index page.
# application.add_url_rule('/', 'index', (lambda: header_text +
#     say_hello() + instructions + footer_text))

# # add a rule when the page is accessed with a name appended to the site
# # URL.
# application.add_url_rule('/<username>', 'hello', (lambda username:
#     header_text + say_hello(username) + home_link + footer_text))

# # run the app.
# if __name__ == "__main__":
#     # Setting debug to True enables debug output. This line should be
#     # removed before deploying a production app.
#     application.debug = True
#     application.run()

@application.route('/simulate', methods=['POST'])
def simulate():
    try:
        request_dict =  request
        response = Response("recieved", status=200)
    except Exception as ex:
        response = Response(str(traceback.format_exc()), status=500)

    return response

if __name__ == '__main__':
    application.run(host='0.0.0.0')