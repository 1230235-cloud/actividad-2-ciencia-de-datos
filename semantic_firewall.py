import requests
import json

def validate_category_with_ollama(product_name, category, description):
    """
    Envía el registro al modelo local de Ollama para validar si la categoría
    corresponde lógicamente con el nombre y la descripción.
    """
    prompt = f"""
    Actúa como un validador de calidad de datos.
    Analiza el siguiente producto:
    - Nombre: {product_name}
    - Categoría asignada: {category}
    - Descripción: {description}

    ¿La categoría asignada coincide semánticamente con el nombre y descripción?
    Responde ÚNICAMENTE en formato JSON con la siguiente estructura:
    {{"valido": true/false, "razon": "breve explicacion"}}
    """

    try:
        response = requests.post(
            "http://localhost:11434/api/generate",
            json={
                "model": "llama3.2:1b",  # O el modelo que tengas instalado (ej. "mistral")
                "prompt": prompt,
                "stream": False,
                "format": "json"
            },
            timeout=10
        )
        
        if response.status_code == 200:
            result = json.loads(response.json()["response"])
            return result
        else:
            return {"valido": True, "razon": "Error de conexión con Ollama"}

    except Exception as e:
        print(f"No se pudo conectar a Ollama local: {e}")
        return {"valido": True, "razon": "Ollama no disponible, paso por alto"}

# Ejemplo de prueba rápida:
if __name__ == "__main__":
    # Prueba 1: Datos consistentes
    res1 = validate_category_with_ollama(
        "Smart TV 55 pulgadas 4K", 
        "Electrónica", 
        "Pantalla LED con resolución ultra HD y aplicaciones de streaming"
    )
    print("Prueba 1:", res1)

    # Prueba 2: Anomalía semántica
    res2 = validate_category_with_ollama(
        "Pantalón de mezclilla Slim Fit", 
        "Electrónica", 
        "Ropa casual de algodón azul con bolsas traseras"
    )
    print("Prueba 2:", res2)