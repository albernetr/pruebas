
import unittest
from datetime import date
from fechas_utiles import (
    meses_y_dias_en_intervalo,
    meses_y_dias_en_anio,
    meses_y_dias_en_rango,
    meses_y_dias_en_anio_desde_str,
)

class TestFechasUtiles(unittest.TestCase):
    def test_intervalo_basico_todo_anio(self):
        self.assertEqual(meses_y_dias_en_intervalo(date(2025,1,1), date(2025,12,31)), (12, 0))

    def test_intervalo_con_dias(self):
        self.assertEqual(meses_y_dias_en_intervalo(date(2025,7,10), date(2025,8,5)), (0, 27))

    def test_fin_exclusivo(self):
        self.assertEqual(meses_y_dias_en_intervalo(date(2025,1,31), date(2025,3,1), fin_inclusivo=False), (1, 1))

    def test_bisiesto_ajuste_fin_de_mes(self):
        # 31/01/2024 a 29/02/2024 (año bisiesto), fin inclusivo: 1 mes y 0 dias
        self.assertEqual(meses_y_dias_en_intervalo(date(2024,1,31), date(2024,2,29)), (1, 0))

    def test_en_anio_traslape(self):
        # 12/15/2024–03/20/2025 dentro de 2025 => 2 meses y 20 días
        self.assertEqual(meses_y_dias_en_anio(date(2024,12,15), date(2025,3,20), 2025), (2, 20))

    def test_wrappers_str(self):
        self.assertEqual(meses_y_dias_en_rango("12/15/2024", "03/20/2025"), (3, 6))
        self.assertEqual(meses_y_dias_en_anio_desde_str("12/15/2024", "03/20/2025", 2025), (2, 20))

if __name__ == '__main__':
    unittest.main()
