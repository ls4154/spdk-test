#include "spdk/stdinc.h"

#include "spdk/nvme.h"
#include "spdk/vmd.h"
#include "spdk/env.h"

#include <stdbool.h>
#include <time.h>
#include <unistd.h>
#include <fcntl.h>

#define SPDK_NVME_OPC_COSMOS_WRITE 0x81
#define SPDK_NVME_OPC_COSMOS_READ  0x82

#define COSMOS_OBJ_SIZE		(4 * 1024 * 1024)
#define COSMOS_OBJ_ALIGN	(0x1000)
#define COSMOS_OBJ_SECTSIZE	(4 * 1024)
#define COSMOS_MAX_OBJNO	(128)

#define NANOSEC_PER_SEC 1000000000
#define BYTE_PER_MB (1024 * 1024)

struct ctrlr_entry {
	struct spdk_nvme_ctrlr	*ctrlr;
	struct ctrlr_entry	*next;
	char			name[1024];
};

struct ns_entry {
	struct spdk_nvme_ctrlr	*ctrlr;
	struct spdk_nvme_ns	*ns;
	struct ns_entry		*next;
	struct spdk_nvme_qpair	*qpair;
};

static struct ctrlr_entry *g_controllers = NULL;
static struct ns_entry *g_namespaces = NULL;

static bool g_vmd = false;

static int g_sectsize;

static bool g_write = true;
static int g_sectcnt = 0;
static int g_qdepth = 1;
static int g_time = 10;
static bool g_rand = false;

static bool g_use_obj = false;

static int g_report_interval = 0;

static int g_cnt;
static int g_cnt_done;
static long long g_latency_sum;

static bool g_done = false;

struct spdk_nvme_cmd g_obj_cmd = {
	.nsid = 1,
};

static void
register_ns(struct spdk_nvme_ctrlr *ctrlr, struct spdk_nvme_ns *ns)
{
	struct ns_entry *entry;

	if (!spdk_nvme_ns_is_active(ns)) {
		return;
	}

	entry = malloc(sizeof(struct ns_entry));
	if (entry == NULL) {
		perror("ns_entry malloc");
		exit(1);
	}

	entry->ctrlr = ctrlr;
	entry->ns = ns;
	entry->next = g_namespaces;
	g_namespaces = entry;

	fprintf(stderr, "  Namespace ID: %d size: %juGB\n", spdk_nvme_ns_get_id(ns),
	       spdk_nvme_ns_get_size(ns) / 1000000000);
}

struct obj_perf_seq {
	struct ns_entry	*ns_entry;
	char		*buf;
	uint64_t	phys_addr;
	int		is_completed;
	struct timespec ts_start;
};

static inline int next_idx(void)
{
	if (g_rand)
		return rand() % COSMOS_MAX_OBJNO;
	else
		return (g_cnt++) % COSMOS_MAX_OBJNO;

}

static void
obj_complete(void *arg, const struct spdk_nvme_cpl *cpl)
{
	struct obj_perf_seq *seq = arg;
	struct timespec ts_cur;
	struct ns_entry* ns_entry = seq->ns_entry;

	if (spdk_nvme_cpl_is_error(cpl)) {
		spdk_nvme_qpair_print_completion(seq->ns_entry->qpair, (struct spdk_nvme_cpl *)cpl);
		fprintf(stderr, "I/O error status: %s\n", spdk_nvme_cpl_get_status_string(&cpl->status));
		fprintf(stderr, "I/O failed, aborting run\n");
		seq->is_completed = 2;
	}
	if (g_done) {
		seq->is_completed = 1;
	} else {
		int rc;
		g_cnt_done++;

		clock_gettime(CLOCK_MONOTONIC, &ts_cur);
		g_latency_sum += 1LL * (ts_cur.tv_sec - seq->ts_start.tv_sec) * NANOSEC_PER_SEC
				 + (ts_cur.tv_nsec - seq->ts_start.tv_nsec);

		clock_gettime(CLOCK_MONOTONIC, &seq->ts_start);
		g_obj_cmd.rsvd2 = (seq->phys_addr & 0xFFFFFFFFULL);
		g_obj_cmd.rsvd3 = (seq->phys_addr >> 32);
		g_obj_cmd.cdw10 = next_idx();
		g_obj_cmd.cdw12 = g_sectcnt - 1;
		rc = spdk_nvme_ctrlr_io_cmd_raw_no_payload_build(ns_entry->ctrlr, ns_entry->qpair, &g_obj_cmd,
								 obj_complete, seq);
		if (rc != 0) {
			fprintf(stderr, "starting obj I/O failed\n");
			exit(1);
		}
	}
}

static void
lba_complete(void *arg, const struct spdk_nvme_cpl *cpl)
{
	struct obj_perf_seq *seq = arg;
	struct timespec ts_cur;
	struct ns_entry* ns_entry = seq->ns_entry;


	if (spdk_nvme_cpl_is_error(cpl)) {
		spdk_nvme_qpair_print_completion(seq->ns_entry->qpair, (struct spdk_nvme_cpl *)cpl);
		fprintf(stderr, "I/O error status: %s\n", spdk_nvme_cpl_get_status_string(&cpl->status));
		fprintf(stderr, "I/O failed, aborting run\n");
		seq->is_completed = 2;
	}
	if (g_done) {
		seq->is_completed = 1;
	} else {
		int rc;
		g_cnt_done++;

		clock_gettime(CLOCK_MONOTONIC, &ts_cur);
		g_latency_sum += 1LL * (ts_cur.tv_sec - seq->ts_start.tv_sec) * NANOSEC_PER_SEC
				 + (ts_cur.tv_nsec - seq->ts_start.tv_nsec);

		clock_gettime(CLOCK_MONOTONIC, &seq->ts_start);
		uint64_t target_lba = next_idx() * (COSMOS_OBJ_SIZE / g_sectsize);
		uint32_t target_cnt = g_sectcnt;
		rc = spdk_nvme_ns_cmd_write(ns_entry->ns, ns_entry->qpair, seq->buf, target_lba,
					    target_cnt, lba_complete, seq, 0);
		if (rc != 0) {
			fprintf(stderr, "starting obj I/O failed\n");
			exit(1);
		}
	}
}

static void
do_rw_obj(void)
{
	struct ns_entry		*ns_entry;
	struct obj_perf_seq	*seq;
	int			rc;

	struct spdk_nvme_io_qpair_opts def_opts;

	ns_entry = g_namespaces;
	if (ns_entry != NULL) {
		if (g_sectcnt == 0) {
			g_sectcnt = COSMOS_OBJ_SIZE / COSMOS_OBJ_SECTSIZE;
		}

		if (g_sectcnt > COSMOS_OBJ_SIZE / COSMOS_OBJ_SECTSIZE) {
			fprintf(stderr, "IO size exceed object size\n");
			return;
		}

		ns_entry->qpair = spdk_nvme_ctrlr_alloc_io_qpair(ns_entry->ctrlr, NULL, 0);
		if (ns_entry->qpair == NULL) {
			fprintf(stderr, "ERROR: spdk_nvme_ctrlr_alloc_io_qpair() failed\n");
			return;
		}

		spdk_nvme_ctrlr_get_default_io_qpair_opts(ns_entry->ctrlr, &def_opts, sizeof(def_opts));
		if (def_opts.io_queue_size < (unsigned)g_qdepth) {
			fprintf(stderr, "ERROR: Ctrl queue size is %u\n", def_opts.io_queue_size);
			return;
		}

		seq = malloc(sizeof(struct obj_perf_seq) * g_qdepth);
		memset(seq, 0, sizeof(struct obj_perf_seq) * g_qdepth); 
		if (seq == NULL) {
			perror("malloc");
			return;
		}

		for (int i = 0; i < g_qdepth; i++) {
			seq[i].ns_entry = ns_entry;
			seq[i].buf = spdk_zmalloc(COSMOS_OBJ_SIZE, COSMOS_OBJ_ALIGN, &seq[i].phys_addr,
							SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);
			if (seq[i].buf == NULL) {
				fprintf(stderr, "ERROR: spdk zmalloc failed\n");
				return;
			}
			seq[i].is_completed = 0;

			// check buffer is contiguous
			if (spdk_vtophys(seq[i].buf + 2 * 1024 * 1024, NULL) !=
					seq[i].phys_addr + 2 * 1024 * 1024) {
				fprintf(stderr, "ERROR: buffer not contiguous\n");
				return;
			}
		}

		g_obj_cmd.opc = g_write ? SPDK_NVME_OPC_COSMOS_WRITE : SPDK_NVME_OPC_COSMOS_READ;

		struct timespec ts_start, ts_last, ts_cur;
		clock_gettime(CLOCK_MONOTONIC, &ts_start);
		ts_last = ts_start;

		// initial io
		for (int i = 0; i < g_qdepth; i++) {
			clock_gettime(CLOCK_MONOTONIC, &seq[i].ts_start);
			g_obj_cmd.rsvd2 = (seq[i].phys_addr & 0xFFFFFFFFULL);
			g_obj_cmd.rsvd3 = (seq[i].phys_addr >> 32);
			g_obj_cmd.cdw10 = next_idx();
			g_obj_cmd.cdw12 = g_sectcnt - 1;
			rc = spdk_nvme_ctrlr_io_cmd_raw_no_payload_build(ns_entry->ctrlr,
									 ns_entry->qpair, &g_obj_cmd,
									 obj_complete, &seq[i]);
			if (rc != 0) {
				fprintf(stderr, "starting obj I/O failed\n");
				exit(1);
			}
		}

		// main loop
		int cnt_done_last = 0;
		while (1) {
			spdk_nvme_qpair_process_completions(ns_entry->qpair, 0);
			clock_gettime(CLOCK_MONOTONIC, &ts_cur);

			long long time_diff = 1LL * (ts_cur.tv_sec - ts_last.tv_sec) * NANOSEC_PER_SEC
					      + (ts_cur.tv_nsec - ts_last.tv_nsec);
			if (g_report_interval != 0 && time_diff >= 1LL * g_report_interval * NANOSEC_PER_SEC) {
				printf(" throughput: %f MB/s\n",
				       (double)(COSMOS_OBJ_SECTSIZE * g_sectcnt) * (g_cnt_done - cnt_done_last)
				       / time_diff / BYTE_PER_MB * NANOSEC_PER_SEC);
				ts_last = ts_cur;
				cnt_done_last = g_cnt_done;
			}

			if (1LL * (ts_cur.tv_sec - ts_start.tv_sec) * NANOSEC_PER_SEC
			    + (ts_cur.tv_nsec - ts_start.tv_nsec) >= 1LL * g_time * NANOSEC_PER_SEC) {
				break;
			}
		}

		g_done = true;

		long long elapsed_time = 1LL * (ts_cur.tv_sec - ts_start.tv_sec) * NANOSEC_PER_SEC
					 + (ts_cur.tv_nsec - ts_start.tv_nsec);
		printf("elapsed time: %lld ns\n",  elapsed_time);
		printf("ops count: %d\n",  g_cnt_done);
		printf("throughput: %f MB/s\n",
		       (double)(COSMOS_OBJ_SECTSIZE * g_sectcnt) * g_cnt_done
		       / elapsed_time / BYTE_PER_MB * NANOSEC_PER_SEC);
		printf("avg latency per op: %f ns\n", (double)g_latency_sum / g_cnt_done);

		// check unfinished io
		for (int i = 0; i < g_qdepth; i++) {
			while (!seq[i].is_completed)
				spdk_nvme_qpair_process_completions(ns_entry->qpair, 0);
			spdk_free(seq[i].buf);
		}

		spdk_nvme_ctrlr_free_io_qpair(ns_entry->qpair);
	}
}

static void
do_rw_lba(void)
{
	struct ns_entry		*ns_entry;
	struct obj_perf_seq	*seq;
	int			rc;

	struct spdk_nvme_io_qpair_opts def_opts;

	ns_entry = g_namespaces;
	if (ns_entry != NULL) {
		g_sectsize = spdk_nvme_ns_get_sector_size(ns_entry->ns);
		printf("Sector size: %d\n", g_sectsize);

		if (g_sectcnt == 0) {
			g_sectcnt = COSMOS_OBJ_SIZE / g_sectsize;
		}

		if (1LL * g_sectsize * g_sectcnt > COSMOS_OBJ_SIZE) {
			fprintf(stderr, "IO size exceed object size\n");
			return;
		}

		ns_entry->qpair = spdk_nvme_ctrlr_alloc_io_qpair(ns_entry->ctrlr, NULL, 0);
		if (ns_entry->qpair == NULL) {
			fprintf(stderr, "ERROR: spdk_nvme_ctrlr_alloc_io_qpair() failed\n");
			return;
		}

		spdk_nvme_ctrlr_get_default_io_qpair_opts(ns_entry->ctrlr, &def_opts, sizeof(def_opts));
		if (def_opts.io_queue_size < (unsigned)g_qdepth) {
			fprintf(stderr, "ERROR: Ctrl queue size is %u\n", def_opts.io_queue_size);
			return;
		}

		seq = malloc(sizeof(struct obj_perf_seq) * g_qdepth);
		memset(seq, 0, sizeof(struct obj_perf_seq) * g_qdepth);
		if (seq == NULL) {
			perror("malloc");
			return;
		}

		for (int i = 0; i < g_qdepth; i++) {
			seq[i].ns_entry = ns_entry;
			seq[i].buf = spdk_zmalloc(COSMOS_OBJ_SIZE, COSMOS_OBJ_ALIGN, &seq[i].phys_addr,
							SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);
			if (seq[i].buf == NULL) {
				fprintf(stderr, "ERROR: spdk zmalloc failed\n");
				return;
			}
			seq[i].is_completed = 0;

			// check buffer is contiguous
			if (spdk_vtophys(seq[i].buf + 2 * 1024 * 1024, NULL) !=
					seq[i].phys_addr + 2 * 1024 * 1024) {
				fprintf(stderr, "ERROR: buffer not contiguous\n");
				return;
			}
		}

		struct timespec ts_start, ts_last, ts_cur;
		clock_gettime(CLOCK_MONOTONIC, &ts_start);
		ts_last = ts_start;

		// initial io
		for (int i = 0; i < g_qdepth; i++) {
			clock_gettime(CLOCK_MONOTONIC, &seq[i].ts_start);
			uint64_t target_lba = next_idx() * (COSMOS_OBJ_SIZE / g_sectsize);
			uint32_t target_cnt = g_sectcnt;
			rc = spdk_nvme_ns_cmd_write(ns_entry->ns, ns_entry->qpair, seq[i].buf,
						    target_lba, target_cnt, lba_complete, &seq[i], 0);
			if (rc != 0) {
				fprintf(stderr, "starting I/O failed\n");
				exit(1);
			}
		}

		// main loop
		int cnt_done_last = 0;
		while (1) {
			spdk_nvme_qpair_process_completions(ns_entry->qpair, 0);
			clock_gettime(CLOCK_MONOTONIC, &ts_cur);

			long long time_diff = 1LL * (ts_cur.tv_sec - ts_last.tv_sec) * NANOSEC_PER_SEC
					      + (ts_cur.tv_nsec - ts_last.tv_nsec);
			if (g_report_interval != 0 && time_diff >= 1LL * g_report_interval * NANOSEC_PER_SEC) {
				printf(" throughput: %f MB/s\n",
				       (double)(g_sectsize * g_sectcnt) * (g_cnt_done - cnt_done_last)
				       / time_diff / BYTE_PER_MB * NANOSEC_PER_SEC);
				ts_last = ts_cur;
				cnt_done_last = g_cnt_done;
			}

			if (1LL * (ts_cur.tv_sec - ts_start.tv_sec) * NANOSEC_PER_SEC
			    + (ts_cur.tv_nsec - ts_start.tv_nsec) >= 1LL * g_time * NANOSEC_PER_SEC) {
				break;
			}
		}

		g_done = true;

		long long elapsed_time = 1LL * (ts_cur.tv_sec - ts_start.tv_sec) * NANOSEC_PER_SEC
					 + (ts_cur.tv_nsec - ts_start.tv_nsec);
		printf("elapsed time: %lld ns\n",  elapsed_time);
		printf("ops count: %d\n",  g_cnt_done);
		printf("throughput: %f MB/s\n",
		       (double)(g_sectsize * g_sectcnt) * g_cnt_done
		       / elapsed_time / BYTE_PER_MB * NANOSEC_PER_SEC);
		printf("avg latency per op: %f ns\n", (double)g_latency_sum / g_cnt_done);

		// check unfinished io
		for (int i = 0; i < g_qdepth; i++) {
			while (!seq[i].is_completed)
				spdk_nvme_qpair_process_completions(ns_entry->qpair, 0);
			spdk_free(seq[i].buf);
		}

		spdk_nvme_ctrlr_free_io_qpair(ns_entry->qpair);
	}
}

static bool
probe_cb(void *cb_ctx, const struct spdk_nvme_transport_id *trid,
	 struct spdk_nvme_ctrlr_opts *opts)
{
	fprintf(stderr, "Attaching to %s\n", trid->traddr);
	return true;
}

static void
attach_cb(void *cb_ctx, const struct spdk_nvme_transport_id *trid,
	  struct spdk_nvme_ctrlr *ctrlr, const struct spdk_nvme_ctrlr_opts *opts)
{
	int nsid, num_ns;
	struct ctrlr_entry *entry;
	struct spdk_nvme_ns *ns;
	const struct spdk_nvme_ctrlr_data *cdata;

	entry = malloc(sizeof(struct ctrlr_entry));
	if (entry == NULL) {
		perror("ctrlr_entry malloc");
		exit(1);
	}

	fprintf(stderr, "Attached to %s\n", trid->traddr);

	cdata = spdk_nvme_ctrlr_get_data(ctrlr);

	snprintf(entry->name, sizeof(entry->name), "%-20.20s (%-20.20s)", cdata->mn, cdata->sn);

	entry->ctrlr = ctrlr;
	entry->next = g_controllers;
	g_controllers = entry;

	num_ns = spdk_nvme_ctrlr_get_num_ns(ctrlr);
	fprintf(stderr, "Using controller %s with %d namespaces.\n", entry->name, num_ns);
	for (nsid = 1; nsid <= num_ns; nsid++) {
		ns = spdk_nvme_ctrlr_get_ns(ctrlr, nsid);
		if (ns == NULL) {
			continue;
		}
		register_ns(ctrlr, ns);
	}
}

static void
cleanup(void)
{
	struct ns_entry *ns_entry = g_namespaces;
	struct ctrlr_entry *ctrlr_entry = g_controllers;

	while (ns_entry) {
		struct ns_entry *next = ns_entry->next;
		free(ns_entry);
		ns_entry = next;
	}

	while (ctrlr_entry) {
		struct ctrlr_entry *next = ctrlr_entry->next;

		spdk_nvme_detach(ctrlr_entry->ctrlr);
		free(ctrlr_entry);
		ctrlr_entry = next;
	}
}

static void
usage(const char *program_name)
{
	fprintf(stderr, "%s [options]", program_name);
	fprintf(stderr, "\n");
	fprintf(stderr, "options:\n");
	fprintf(stderr, " -V         enumerate VMD\n");
	fprintf(stderr, " -w         write\n");
	fprintf(stderr, " -r         read\n");
	fprintf(stderr, " -c SECT    sector count per io (default maximum cnt\n");
	fprintf(stderr, " -q DEPTH   queue depth (default 1)\n");
	fprintf(stderr, " -t TIME    time in seconds (default 10)\n");
	fprintf(stderr, " -l         use lba io command (default)\n");
	fprintf(stderr, " -o         use obj io command\n");
	fprintf(stderr, " -i SEC     throughput report interval, use 0 to disable (default 0)\n");
	fprintf(stderr, " -R         radnom io\n");
}

static int
parse_args(int argc, char **argv)
{
	int op;

	while ((op = getopt(argc, argv, "Vwrc:q:t:loi:R")) != -1) {
		switch (op) {
		case 'V':
			g_vmd = true;
			break;
		case 'w':
			g_write = true;
			break;
		case 'r':
			g_write = false;
			break;
		case 'c':
			g_sectcnt = atoi(optarg);
			if (g_sectcnt <= 0) {
				fprintf(stderr, "wrong sector count\n");
				return 1;
			}
			break;
		case 'q':
			g_qdepth = atoi(optarg);
			if (g_qdepth < 0 || g_qdepth > 1023) {
				fprintf(stderr, "wrong queue depth\n");
				return 1;
			}
			break;
		case 't':
			g_time = atoi(optarg);
			break;
		case 'l':
			g_use_obj = false;
			break;
		case 'o':
			g_use_obj = true;
			break;
		case 'i':
			g_report_interval = atoi(optarg);
			if (g_report_interval < 0) {
				fprintf(stderr, "wrong interval\n");
				return 1;
			}
			break;
		case 'R':
			g_rand = true;
			break;
		default:
			usage(argv[0]);
			return 1;
		}
	}

	return 0;
}

int main(int argc, char **argv)
{
	int rc;
	struct spdk_env_opts opts;

	rc = parse_args(argc, argv);
	if (rc != 0)
		return rc;

	spdk_env_opts_init(&opts);
	opts.name = "nvme_obj_perf";
	opts.shm_id = 0;
	if (spdk_env_init(&opts) < 0) {
		fprintf(stderr, "Unable to initialize SPDK env\n");
		return 1;
	}

	fprintf(stderr, "Initializing NVMe Controllers\n");

	if (g_vmd && spdk_vmd_init()) {
		fprintf(stderr, "Failed to initialize VMD."
			" Some NVMe devices can be unavailable.\n");
	}

	rc = spdk_nvme_probe(NULL, NULL, probe_cb, attach_cb, NULL);
	if (rc != 0) {
		fprintf(stderr, "spdk_nvme_probe() failed\n");
		cleanup();
		return 1;
	}

	if (g_controllers == NULL) {
		fprintf(stderr, "no NVMe controllers found\n");
		cleanup();
		return 1;
	}

	fprintf(stderr, "Initialization complete.\n");
	if (g_use_obj)
		do_rw_obj();
	else
		do_rw_lba();
	cleanup();
	if (g_vmd)
		spdk_vmd_fini();

	return 0;
}
