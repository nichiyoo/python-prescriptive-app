# K-Pop Concert Budget Recommendation System

Prescriptive analytics system for K-pop concert budget planning with data lakehouse architecture.

## Structure

```
kpop-budget/
├── config/
│   └── settings.py      # Load .env config
├── core/
│   ├── storage.py       # MinIO operations
│   ├── lakehouse.py     # Bronze/Silver/Gold layers
│   └── prescriptive.py  # Analytics engine
├── gui/
│   └── app.py           # Tkinter GUI
├── docker/
│   ├── docker-compose.yaml  # MinIO setup
│   └── README.md            # Docker guide
└── main.py              # Entry point
```

## Quick Start

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Setup Environment

```bash
cp .env.example .env
```

### 3. Start MinIO (Optional)

```bash
cd docker
docker-compose up -d
```

The bucket `kpop-budget` is created automatically!

### 4. Run Application

```bash
python main.py
```

## Storage Modes

**Local Only (No MinIO needed):**

```env
USE_LOCAL_STORAGE=true
```

Files saved to `data/bronze|silver|gold/`

**MinIO Only:**

```env
USE_LOCAL_STORAGE=false
```

Files saved to MinIO bucket `kpop-budget`

## MinIO Access

After running docker-compose:

- **Console**: http://localhost:9001
- **API**: http://localhost:9000
- **Login**: minioadmin / minioadmin

## Usage

1. Click "Load CSV (Bronze)" to upload concert data
2. Enter your budget
3. Click "Analisis Prescriptive" for recommendations
4. View top 10 concerts sorted by score

## CSV Format

Required columns:

- `nama_konser`, `lokasi`, `tanggal`
- `harga_tiket`, `biaya_transport`, `biaya_akomodasi`
- `merchandise`, `total_pengeluaran`

## Prescriptive Scoring

**Components:**

- Cost efficiency (40%)
- Budget remaining (30%)
- Experience value (30%)

## Stop MinIO

```bash
cd docker
docker-compose down
```

Remove data volume:

```bash
docker-compose down -v
```
