from flask import Flask, request, Response
import traceback
import psycopg2
# import functions
# import json
# import pandas as pd




application = Flask(__name__)



@application.route('/simulate', methods=['POST'])
def simulate():


    # import psycopg2


    try:
        # GET PARAMETERS

        # SAVE RESULT IN DATABASE 
        connection = psycopg2.connect(user="dbadmin", password="rUWFidoMnk0SulVl4u9C", host="aa1pbfgh471h051.cee9izytbdnd.eu-central-1.rds.amazonaws.com", port="5432", database="postgres")
        cursor = connection.cursor()
        sql_statement = '''INSERT INTO tested_simulation_parameters (simulation_id, run_number, batch_number, priors_dict, simulation_results) VALUES 
                                (1, 2, 3, '{"test":1}', '{"test":2}');
                        '''

        cursor.execute(sql_statement)
        connection.commit()

        return Response('{}', status=200, mimetype='application/json')
    except Exception as ex:
        return Response('{}', status=400, mimetype='application/json')






# @application.route('/simulate2', methods=['POST'])
# def simulate():

#     import psycopg2

#     try:
#         # GET PARAMETERS
#         request_dict = request.data
#         simulation_id = request_dict['simulation_id']
#         run_number = request_dict['run_number']
#         batch_number = request_dict['batch_number']
#         print('executing sim%s run%s batch%s' % (simulation_id, run_number, batch_number))
#         df_dict = request_dict['df_dict']
#         df = pd.DataFrame(df_dict)
#         rules = request_dict['rules']
#         priors_dict = request_dict['priors_dict']
#         batch_size = request_dict['batch_size']
#         y0_values = request_dict['y0_values']
#         is_timeseries_analysis = request_dict['is_timeseries_analysis']
#         times = request_dict['times']
#         timestep_size = request_dict['timestep_size']
#         y0_columns = request_dict['y0_columns']
#         parameter_columns = request_dict['parameter_columns']
#         y0_column_dt = request_dict['y0_column_dt']
#         error_threshold = request_dict['error_threshold']
    
        

        
#         # RUN SIMULATION AND CHECK CORRECTNESS
#         y0_values_in_simulation = functions.likelihood_learning_simulator(df, rules, priors_dict, batch_size, is_timeseries_analysis, times, timestep_size, y0_columns, parameter_columns)
#         errors_dict = functions.n_dimensional_distance(y0_values_in_simulation, y0_values, y0_columns, y0_column_dt,error_threshold, rules) 

#         simulation_results = {}
#         for rule in rules:
#             if rule['learn_posterior']:
#                 simulation_results['nb_of_sim_in_which_rule_' + str(rule['id']) + '_was_used'] = errors_dict[rule['id']]['nb_of_sim_in_which_rule_was_used']
#                 simulation_results['error_rule' + str(rule['id'])] = errors_dict[rule['id']]['error'] 




#         # SAVE RESULT IN DATABASE 
#         connection = psycopg2.connect(user="dbadmin", password="rUWFidoMnk0SulVl4u9C", host="aa1pbfgh471h051.cee9izytbdnd.eu-central-1.rds.amazonaws.com", port="5432", database="postgres")
#         cursor = connection.cursor()
#         sql_statement = '''INSERT INTO tested_simulation_parameters (simulation_id, run_number, batch_number, priors_dict, simulation_results) VALUES 
#                                 (%s, %s, %s, %s, %s);
#                         ''' % (simulation_id, run_number, batch_number, json.dumps(priors_dict), json.dumps(simulation_results))

#         cursor.execute(sql_statement)
#         connection.commit()


#         return Response('{}', status=200, mimetype='application/json')
#     except Exception as ex:
#         return Response('{}', status=400, mimetype='application/json')






if __name__ == '__main__':
    application.run(host='0.0.0.0')