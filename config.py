import os
import configparser


# def fyers_retrive_config(user):
#     # Create a configuration file
#     script_directory = os.path.dirname(os.path.abspath(__file__))
#     config_file_path = os.path.join(script_directory, "config.ini")
#     #    config_file_path = r"E:\arpit_project\config.ini"

#     config = configparser.ConfigParser()
#     config.read(config_file_path)
#     print(config.get(user, "redirecturl", fallback=""))
#     default_values = {
#         "redirecturl": config.get(user, "redirecturl", fallback=""),
#         "apikey": config.get(user, "apikey", fallback=""),
#         "secretkey": config.get(user, "secretkey", fallback=""),
#         "granttype": config.get(user, "granttype", fallback=""),
#         "responsetype": config.get(user, "responsetype", fallback=""),
#     }
#     return default_values


# def fyers_retrieve_all_users():
#     # Get the directory of the current script
#     script_directory = os.path.dirname(os.path.abspath(__file__))
#     config_file_path = os.path.join(script_directory, "config.ini")

#     # Load the configuration file
#     config = configparser.ConfigParser()
#     config.read(config_file_path)

#     # Dictionary to hold all user details
#     all_users_details = {}

#     # Iterate through all sections (assuming each section represents a user)
#     for user in config.sections():
#         all_users_details[user] = {
#             "redirecturl": config.get(user, "redirecturl", fallback=""),
#             "apikey": config.get(user, "apikey", fallback=""),
#             "secretkey": config.get(user, "secretkey", fallback=""),
#             "granttype": config.get(user, "granttype", fallback=""),
#             "responsetype": config.get(user, "responsetype", fallback=""),
#         }
#     return all_users_details


# def save_fyers_config(username, perameter):
#     script_directory = os.path.dirname(os.path.abspath(__file__))
#     config_file_path = os.path.join(script_directory, "config.ini")
#     config = configparser.ConfigParser()
#     config.read(config_file_path)
#     config[username] = perameter
#     with open(config_file_path, "w") as configfile:
#         config.write(configfile)
#     print("Configurations saved to config.ini")




# def save_tradingmode_limit_config(username , perameter):
#     script_directory = os.path.dirname(os.path.abspath(__file__))
#     config_file_path = os.path.join(script_directory, "tradingmode_limit.ini")
#     config = configparser.ConfigParser()
#     config.read(config_file_path)
#     config[username] = perameter
#     with open(config_file_path, 'w') as configfile:
#         config.write(configfile)
#     print("Configurations saved to tradingmode_limit.ini")
