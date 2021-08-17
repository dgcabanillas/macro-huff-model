def get_param( param ):
    params = {
        'gcp_project':  'inretail-negocios-sd',
        'data_set':     'intercorpretail_automatizaciones'
    }
    params = {
        'gcp_project':  'intercorp-dev',
        'data_set':     'datos_demanda'
    }
    return params.get(param, '')

def get_inputs(args):
    #Obtiene los argumentos dados en la linea de comandos
    inputs = {}
    for i in range (1, len(args)):
        [key, value] = args[i].split("=")
        inputs[key] = value
    return inputs

def validate_inputs(args, inputs):
    #valida que todas las variables dadas en "args" se encuentren en "inputs"
    isValid = True
    for arg in args:
        if arg not in inputs or inputs[arg] == '':
            print('- FALTA ARGUMENTO ' + arg.upper())
            isValid = False
    
    if not isValid:
        print ('\nProceso interrumpido.')
    return isValid

def print_args(inputs):
    #imprime todos los argumentos de "inputs"
    print("Argumentos:")
    for arg in inputs:
        print("\t" + arg + ":\t" + inputs[arg])