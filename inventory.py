import os
import csv
import json
from datetime import datetime
from collections import defaultdict, Counter
import argparse

def get_file_size_formatted(size_bytes):
    """
    Convierte el tamaño en bytes a un formato legible
    """
    if size_bytes == 0:
        return "0 B"
    
    size_names = ["B", "KB", "MB", "GB", "TB"]
    i = 0
    while size_bytes >= 1024 and i < len(size_names) - 1:
        size_bytes /= 1024.0
        i += 1
    
    return f"{size_bytes:.2f} {size_names[i]}"

def get_file_info(file_path):
    """
    Obtiene información detallada de un archivo
    """
    try:
        stat_info = os.stat(file_path)
        file_size = stat_info.st_size
        modified_time = datetime.fromtimestamp(stat_info.st_mtime)
        created_time = datetime.fromtimestamp(stat_info.st_ctime)
        
        return {
            'size_bytes': file_size,
            'size_formatted': get_file_size_formatted(file_size),
            'modified_date': modified_time.strftime('%Y-%m-%d %H:%M:%S'),
            'created_date': created_time.strftime('%Y-%m-%d %H:%M:%S')
        }
    except OSError:
        return {
            'size_bytes': 0,
            'size_formatted': '0 B',
            'modified_date': 'N/A',
            'created_date': 'N/A'
        }

def scan_directory(root_path, show_progress=True):
    """
    Escanea recursivamente un directorio y recopila información de todos los archivos y carpetas
    """
    inventory = {
        'scan_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'root_path': os.path.abspath(root_path),
        'folders': [],
        'files': [],
        'summary': {
            'total_folders': 0,
            'total_files': 0,
            'total_size_bytes': 0,
            'extensions': Counter(),
            'largest_files': [],
            'folder_depths': Counter()
        }
    }
    
    if not os.path.exists(root_path):
        print(f"Error: La ruta '{root_path}' no existe.")
        return None
    
    print(f"Iniciando escaneo de: {root_path}")
    print("-" * 50)
    
    file_count = 0
    folder_count = 0
    
    try:
        for root, dirs, files in os.walk(root_path):
            # Calcular profundidad de la carpeta
            depth = root.replace(root_path, '').count(os.sep)
            inventory['summary']['folder_depths'][depth] += 1
            
            # Información de la carpeta actual
            relative_path = os.path.relpath(root, root_path)
            if relative_path != '.':
                folder_info = {
                    'path': root,
                    'relative_path': relative_path,
                    'depth': depth,
                    'subfolders': len(dirs),
                    'files_count': len(files)
                }
                inventory['folders'].append(folder_info)
                folder_count += 1
            
            # Procesar archivos en la carpeta actual
            for file in files:
                file_count += 1
                file_path = os.path.join(root, file)
                relative_file_path = os.path.relpath(file_path, root_path)
                
                # Obtener extensión
                _, extension = os.path.splitext(file)
                extension = extension.lower() if extension else 'sin_extension'
                
                # Obtener información del archivo
                file_info = get_file_info(file_path)
                
                file_data = {
                    'name': file,
                    'path': file_path,
                    'relative_path': relative_file_path,
                    'extension': extension,
                    'folder': root,
                    'relative_folder': os.path.relpath(root, root_path),
                    'depth': depth,
                    **file_info
                }
                
                inventory['files'].append(file_data)
                inventory['summary']['total_size_bytes'] += file_info['size_bytes']
                inventory['summary']['extensions'][extension] += 1
                
                # Mostrar progreso cada 100 archivos
                if show_progress and file_count % 100 == 0:
                    print(f"Procesados: {file_count} archivos, {folder_count} carpetas...")
    
    except KeyboardInterrupt:
        print("\nEscaneo interrumpido por el usuario.")
        return None
    except Exception as e:
        print(f"Error durante el escaneo: {e}")
        return None
    
    # Actualizar resumen
    inventory['summary']['total_folders'] = folder_count
    inventory['summary']['total_files'] = file_count
    inventory['summary']['total_size_formatted'] = get_file_size_formatted(
        inventory['summary']['total_size_bytes']
    )
    
    # Encontrar los archivos más grandes (top 10)
    largest_files = sorted(inventory['files'], 
                          key=lambda x: x['size_bytes'], 
                          reverse=True)[:10]
    inventory['summary']['largest_files'] = [
        {
            'name': f['name'],
            'path': f['relative_path'],
            'size': f['size_formatted'],
            'extension': f['extension']
        }
        for f in largest_files
    ]
    
    print(f"\nEscaneo completado!")
    print(f"Total de archivos: {file_count}")
    print(f"Total de carpetas: {folder_count}")
    print(f"Tamaño total: {inventory['summary']['total_size_formatted']}")
    
    return inventory

def save_inventory_csv(inventory, output_file):
    """
    Guarda el inventario en formato CSV
    """
    try:
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['name', 'relative_path', 'extension', 'size_formatted', 
                         'size_bytes', 'modified_date', 'created_date', 'depth']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            writer.writeheader()
            for file_info in inventory['files']:
                writer.writerow({
                    'name': file_info['name'],
                    'relative_path': file_info['relative_path'],
                    'extension': file_info['extension'],
                    'size_formatted': file_info['size_formatted'],
                    'size_bytes': file_info['size_bytes'],
                    'modified_date': file_info['modified_date'],
                    'created_date': file_info['created_date'],
                    'depth': file_info['depth']
                })
        print(f"Inventario guardado en CSV: {output_file}")
    except Exception as e:
        print(f"Error al guardar CSV: {e}")

def save_inventory_json(inventory, output_file):
    """
    Guarda el inventario completo en formato JSON
    """
    try:
        # Convertir Counter a dict para JSON
        inventory_copy = inventory.copy()
        inventory_copy['summary']['extensions'] = dict(inventory['summary']['extensions'])
        inventory_copy['summary']['folder_depths'] = dict(inventory['summary']['folder_depths'])
        
        with open(output_file, 'w', encoding='utf-8') as jsonfile:
            json.dump(inventory_copy, jsonfile, indent=2, ensure_ascii=False)
        print(f"Inventario completo guardado en JSON: {output_file}")
    except Exception as e:
        print(f"Error al guardar JSON: {e}")

def print_summary(inventory):
    """
    Muestra un resumen del inventario
    """
    summary = inventory['summary']
    
    print("\n" + "="*60)
    print("RESUMEN DEL INVENTARIO")
    print("="*60)
    print(f"Ruta escaneada: {inventory['root_path']}")
    print(f"Fecha de escaneo: {inventory['scan_date']}")
    print(f"Total de archivos: {summary['total_files']:,}")
    print(f"Total de carpetas: {summary['total_folders']:,}")
    print(f"Tamaño total: {summary['total_size_formatted']}")
    
    print(f"\nEXTENSIONES MÁS COMUNES:")
    for ext, count in summary['extensions'].most_common(10):
        percentage = (count / summary['total_files']) * 100
        print(f"  {ext:<15} {count:>8,} archivos ({percentage:>5.1f}%)")
    
    print(f"\nARCHIVOS MÁS GRANDES:")
    for i, file_info in enumerate(summary['largest_files'], 1):
        print(f"  {i:2}. {file_info['name']} ({file_info['size']}) - {file_info['extension']}")
    
    print(f"\nDISTRIBUCIÓN POR PROFUNDIDAD:")
    for depth in sorted(summary['folder_depths'].keys()):
        count = summary['folder_depths'][depth]
        print(f"  Nivel {depth}: {count} carpetas")

def main():
    parser = argparse.ArgumentParser(description='Inventario recursivo de archivos y carpetas')
    parser.add_argument('path', nargs='?', default='C:\\Users\\letorres\\tutorials\\', 
                       help='Ruta a escanear (por defecto: directorio actual)')
    parser.add_argument('--output', '-o', default='inventory', 
                       help='Nombre base para los archivos de salida')
    parser.add_argument('--no-csv', action='store_true', 
                       help='No generar archivo CSV')
    parser.add_argument('--no-json', action='store_true', 
                       help='No generar archivo JSON')
    parser.add_argument('--quiet', '-q', action='store_true', 
                       help='Modo silencioso (sin mostrar progreso)')
    
    args = parser.parse_args()
    
    # Realizar el escaneo
    inventory = scan_directory(args.path, show_progress=not args.quiet)
    
    if inventory is None:
        return
    
    # Mostrar resumen
    print_summary(inventory)
    
    # Guardar archivos de salida
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    if not args.no_csv:
        csv_filename = f"{args.output}_{timestamp}.csv"
        save_inventory_csv(inventory, csv_filename)
    
    if not args.no_json:
        json_filename = f"{args.output}_{timestamp}.json"
        save_inventory_json(inventory, json_filename)

if __name__ == "__main__":
    main()
