import matplotlib.pyplot as plt
import numpy as np

# РЕАЛЬНЫЕ ДАННЫЕ (усреднённые по всем запускам)
sizes = [50000, 100000, 200000]

# Write throughput (усреднено)
range_write  = [50000,  27281,  21039]   # из всех запусков
hashed_write = [57000,  31760,  53072]


# Для наглядности добавим линию с провалами range
range_min = [43086, 22178, 14232]   # худшие запуски

plt.figure(figsize=(11, 7))

plt.plot(sizes, range_write,  'o-',  label='Range (timestamp) — среднее', color='red', linewidth=2.5)
plt.plot(sizes, hashed_write, 's-',  label='Hashed (_id) — среднее', color='green', linewidth=2.5)
plt.plot(sizes, range_min,    'x--', label='Range — худшие запуски (hotspot)', color='darkred', alpha=0.7)

plt.xlabel('Количество документов / операций', fontsize=12)
plt.ylabel('Throughput (ops/sec)', fontsize=12)
plt.title('Сравнение производительности шардинга\nRange (timestamp) vs Hashed (_id)', fontsize=14)
plt.grid(True, linestyle='--', alpha=0.7)
plt.legend(fontsize=11)
plt.tight_layout()

# Сохраняем график в папку report
plt.savefig('report/throughput_comparison.png', dpi=300)
plt.show()

print("График сохранён: report/throughput_comparison.png")
