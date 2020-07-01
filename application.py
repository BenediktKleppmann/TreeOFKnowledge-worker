from flask import Flask
from flask import request, Response
from flask import make_response
import traceback
import psycopg2



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




# with app.simulate('/simulate', method='POST'):
#     try:
#         request_dict = request.json
#         response = Response("received", status=201)
#     except Exception as ex:
#         response = Response(str(traceback.format_exc()), status=500)

#     return response



@application.route('/simulate', methods=['POST'])
def simulate():


    import psycopg2
    connection = psycopg2.connect(user="dbadmin", password="rUWFidoMnk0SulVl4u9C", host="aa1pbfgh471h051.cee9izytbdnd.eu-central-1.rds.amazonaws.com", port="5432", database="postgres")
    cursor = connection.cursor()
    cursor.execute('''INSERT INTO "tested_simulation_parameters" (simulation_id, run, parameter_value, is_valid) VALUES (144, 1, 0.2848569, 'true');''')



    try:
        request_dict = request.data
        print(str(request_dict))
        # return 'received'
        return Response('{}', status=200, mimetype='application/json')
    except Exception as ex:
        # response = make_response(str(traceback.format_exc()), 500)
        return Response('{}', status=400, mimetype='application/json')
        # return 'not received'




if __name__ == '__main__':
    application.run(host='0.0.0.0')