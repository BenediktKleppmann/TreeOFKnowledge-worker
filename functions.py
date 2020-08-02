import pandas as pd
import numpy as np
from scipy.stats import rv_histogram
import json


def likelihood_learning_simulator(df_original, rules, priors_dict, batch_size, is_timeseries_analysis, times, timestep_size, y0_columns, parameter_columns):
    print('---- likelihood_learning_simulator ----')
    df = df_original.copy()

    print('3.1')
    for rule_nb in range(len(rules)):
        rules[rule_nb]['rule_was_used_in_simulation'] = [False]*batch_size
        rule = rules[rule_nb]
        if rule['learn_posterior']:
            if not rule['has_probability_1']:
                df['triggerThresholdForRule' + str(rule['id'])] = priors_dict['triggerThresholdForRule' + str(rule['id'])]
            for used_parameter_id in rule['used_parameter_ids']:
                df['param' + str(used_parameter_id)] = priors_dict['param' + str(used_parameter_id)]
        else:
            if not rule['has_probability_1']:
                df['triggerThresholdForRule' + str(rule['id'])] =  rv_histogram(rule['histogram']).rvs(size=batch_size)
            for used_parameter_id in rule['used_parameter_ids']:
                df['param' + str(used_parameter_id)] = rv_histogram(rule['parameters'][str(used_parameter_id)]['histogram']).rvs(size=batch_size)


    if is_timeseries_analysis: 
        df['delta_t'] = timestep_size
    else:
        df[y0_columns] = None

    print('3.2')
    y0_values_in_simulation = pd.DataFrame(index=range(batch_size))
    for period in range(len(times[1:])):
        df['randomNumber'] = np.random.random(batch_size)
        for rule in rules:
            populated_df_rows = pd.Series([True] * len(df))
            for used_column in rule['used_columns']:
                populated_df_rows = populated_df_rows & ~df[used_column].isna()
            populated_df = df[populated_df_rows]

            if rule['is_conditionless']:
                condition_satisfying_rows = pd.Series([True] * batch_size)
                if rule['has_probability_1']:
                    satisfying_rows = pd.Series([True] * batch_size)
                else:
                    satisfying_rows = pd.eval('df.randomNumber < df.triggerThresholdForRule' + str(rule['id']))
                    
            else:
                condition_satisfying_rows = pd.Series([False] * batch_size)

                if len(populated_df)==0:
                    satisfying_rows = populated_df_rows
                if rule['has_probability_1']:
                    condition_satisfying_rows[populated_df_rows] = pd.eval(rule['condition_exec'])
                    if condition_satisfying_rows.iloc[0] in [-1,-2]: #messy bug-fix for bug where eval returns -1 and -2 instead of True and False
                        condition_satisfying_rows += 2
                        condition_satisfying_rows = condition_satisfying_rows.astype(bool)
                    satisfying_rows = condition_satisfying_rows
                else:
                    condition_satisfying_rows[populated_df_rows] = pd.eval(rule['condition_exec'])
                    triggered_rules = pd.eval('df.randomNumber < df.triggerThresholdForRule' + str(rule['id']))
                    satisfying_rows = condition_satisfying_rows & triggered_rules 

                # fix for: conditions with randomNumber/param+ might be basically satisfied except for the random number
                if 'df.randomNumber' in rule['condition_exec'] or 'df.param' in rule['condition_exec']:
                    condition_satisfying_rows = pd.Series([True] * batch_size)


            # --------  used rules  --------
            if rule['learn_posterior']:
                rule['rule_was_used_in_simulation'] = rule['rule_was_used_in_simulation'] | condition_satisfying_rows


            # --------  THEN  --------
            if rule['effect_is_calculation']: 
                new_values = pd.eval(rule['effect_exec'])
                if rule['changed_var_data_type'] in ['relation','int']:
                    nan_rows = new_values.isnull()
                    new_values = new_values.fillna(0)
                    new_values = new_values.astype(int)
                    new_values[nan_rows] = np.nan
                elif rule['changed_var_data_type'] == 'real':
                    new_values = new_values.astype(float)
                # elif rule['changed_var_data_type'] in ['boolean','bool']:
                elif rule['changed_var_data_type'] in ['string','date']:
                    nan_rows = new_values.isnull()
                    new_values = new_values.astype(str)
                    new_values[nan_rows] = np.nan

            else:
                # new_values = rule['effect_exec']
                new_values = pd.Series([rule['effect_exec']] * batch_size)


            # df.loc[satisfying_rows,rule['column_to_change']] = new_values 
            satisfying_rows[satisfying_rows.isna()] = False
            new_values[np.logical_not(satisfying_rows)] = df.loc[np.logical_not(satisfying_rows),rule['column_to_change']]
            new_values[new_values.isna()] = df.loc[new_values.isna(),rule['column_to_change']]
            df[rule['column_to_change']] = new_values


        y0_values_in_this_period = pd.DataFrame(df[y0_columns])
        y0_values_in_this_period.columns = [col + 'period' + str(period+1) for col in y0_values_in_this_period.columns] #faster version
        y0_values_in_simulation = y0_values_in_simulation.join(y0_values_in_this_period)
        print('3.3 - ' + str(period))

    print('3.4')
    for rule in rules:  
        if rule['learn_posterior']:
            y0_values_in_simulation['rule_used_in_simulation_' + str(rule['id'])] = rule['rule_was_used_in_simulation']
            del rule['rule_was_used_in_simulation']


    print('=========================================')
    print('y0_values_in_simulation:' + str(y0_values_in_simulation.columns))
    print('parameter_columns:' + str(parameter_columns))
    print('df.columns:' + str(df.columns))
    print('=========================================')
    y0_values_in_simulation = pd.concat([y0_values_in_simulation,df[parameter_columns]], axis=1)
    y0_values_in_simulation.index = range(len(y0_values_in_simulation))
    return y0_values_in_simulation.to_dict('records')








def n_dimensional_distance(u, v, y0_columns, y0_column_dt,error_threshold, rules):
    print('------------  n_dimensional_distance  ---------------------')
    # u = simulated values;  v = correct_values
    u = np.asarray(u, dtype=object, order='c').squeeze()
    u = np.atleast_1d(u)
    v = np.asarray(v, dtype=object, order='c').squeeze()
    v = np.atleast_1d(v)
    u_df = pd.DataFrame(list(u))
    v_df = pd.DataFrame(list(v))
    u_df = u_df.fillna(np.nan)
    v_df = v_df.fillna(np.nan)


    total_error = np.zeros(shape=len(u))
    dimensionality = np.zeros(shape=len(u))
    for y0_column in y0_columns:
        period_columns = [col for col in u_df.columns if col.split('period')[0] == y0_column]
        if y0_column_dt[y0_column] in ['string','bool','relation']:
            for period_column in period_columns:
                error = 1. - np.equal(np.array(u_df[period_column]), np.array(v_df[period_column])).astype(int)
                error[pd.isnull(v_df[period_column])] = 0 # set the error to zero where the correct value was not given
                error[pd.isnull(u_df[period_column])] = 0 # set the error to zero where the simulation value was not given
                total_error += error
                dimensionality += 1 - np.array(np.logical_or(v_df[period_column].isnull(),u_df[period_column].isnull()).astype(int))
        elif y0_column_dt[y0_column] in ['int','real']:
            for period_column in period_columns:
                period_number = max(int(period_column.split('period')[1]), 1)
  
                # relative_change = np.abs(np.array(u_df[period_column]) - np.array(v_df[period_column.split('period')[0]]))/period_number
                # normalisation_factor = np.maximum(np.abs(u_df[period_column]),np.abs(v_df[period_column.split('period')[0]]))
                # normalisation_factor = np.maximum(normalisation_factor, 1)
                # relative_change = relative_change/normalisation_factor
                # # relative_change_non_null = np.nan_to_num(relative_change, nan=1.0)  

                # absolute_change = np.abs(np.array(u_df[period_column]) - np.array(v_df[period_column.split('period')[0]]))
                # absolute_change = absolute_change/np.abs(np.percentile(absolute_change, 30))
                # # absolute_change_non_null = np.nan_to_num(absolute_change, nan=1.0) 

                residuals = np.abs(np.array(u_df[period_column]) - np.array(v_df[period_column]))
                non_null_residuals = residuals[~np.isnan(residuals)]
                nth_percentile = np.percentile(non_null_residuals, error_threshold*100) if len(non_null_residuals) > 0 else 1# whereby n is the error_threshold. It therefore automatically adapts to the senistivity...
                error_divisor = nth_percentile if nth_percentile != 0 else 1
                error_in_error_range =  residuals/error_divisor
                # pdb.set_trace()
                error_in_error_range_non_null = np.nan_to_num(error_in_error_range, nan=0)  
                error_in_error_range_non_null = np.minimum(error_in_error_range_non_null, 1)

                true_change_factor = (np.array(v_df[period_column])/np.array(v_df[period_column.split('period')[0]]))
                true_change_factor_per_period = np.power(true_change_factor, (1/period_number))
                simulated_change_factor = (np.array(u_df[period_column])/np.array(v_df[period_column.split('period')[0]]))
                simulated_change_factor_per_period = np.power(simulated_change_factor, (1/period_number))
                error_of_value_change = np.abs(simulated_change_factor_per_period - true_change_factor_per_period) 
                error_of_value_change_non_null = np.nan_to_num(error_of_value_change, nan=0)  
                error_of_value_change_non_null = np.minimum(error_of_value_change_non_null, 1)

                error = 0.5*np.minimum(error_in_error_range_non_null,error_of_value_change_non_null) + 0.25*np.sqrt(error_in_error_range_non_null) + 0.25*np.sqrt(error_of_value_change_non_null)
                
                null_value_places = np.logical_or(np.isnan(error_in_error_range), np.isnan(error_of_value_change))
                error[null_value_places] = 0

                dimensionality += 1 - null_value_places.astype('int')
                total_error += error 

    non_validated_rows = dimensionality == 0
    dimensionality = np.maximum(dimensionality, [1]*len(u))
    error = total_error/dimensionality
    error[non_validated_rows] = 1



    errors_dict = {'all_errors':error}
    for rule in rules:
        if rule['learn_posterior']:
            rule_used_in_simulation = u_df['rule_used_in_simulation_' + str(rule['id'])]
            errors_dict[rule['id']] = {'error': error[rule_used_in_simulation].mean(), 'nb_of_sim_in_which_rule_was_used': rule_used_in_simulation.sum()}

    return errors_dict