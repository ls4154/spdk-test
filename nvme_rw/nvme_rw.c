#include "spdk/stdinc.h"

#include "spdk/nvme.h"
#include "spdk/vmd.h"
#include "spdk/env.h"

#include <time.h>
#include <unistd.h>
#include <fcntl.h>

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

static bool g_write = 0;
static char *g_fpath = NULL;
static int g_fd;
static uint64_t g_startlba = 0;
static uint32_t g_lbacnt = 1;

static unsigned int g_sectsize;

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

struct nvme_rw_ctx {
	struct ns_entry	*ns_entry;
	char		*buf;
	uint64_t	phys_addr;
	int		is_completed;
	struct timespec ts_start;
};

static void
read_complete(void *arg, const struct spdk_nvme_cpl *completion)
{
	struct nvme_rw_ctx *ctx = arg;
	int wcnt;
	struct timespec ts_cur;

	clock_gettime(CLOCK_MONOTONIC, &ts_cur);
	fprintf(stderr, "latency: %lld ns\n", 1LL * (ts_cur.tv_sec - ctx->ts_start.tv_sec) * 1000000000
					+ (ts_cur.tv_nsec - ctx->ts_start.tv_nsec));

	ctx->is_completed = 1;
	if (spdk_nvme_cpl_is_error(completion)) {
		spdk_nvme_qpair_print_completion(ctx->ns_entry->qpair,
						 (struct spdk_nvme_cpl *)completion);
		fprintf(stderr, "I/O error status: %s\n",
				spdk_nvme_cpl_get_status_string(&completion->status));
		fprintf(stderr, "Read I/O failed, aborting run\n");
		ctx->is_completed = 2;
	}

	wcnt = write(g_fd, ctx->buf, g_sectsize * g_lbacnt);
	fprintf(stderr, "%d bytes\n", wcnt);

	spdk_free(ctx->buf);
}

static void
write_complete(void *arg, const struct spdk_nvme_cpl *completion)
{
	struct nvme_rw_ctx *ctx = arg;
	struct timespec ts_cur;

	clock_gettime(CLOCK_MONOTONIC, &ts_cur);
	fprintf(stderr, "latency: %lld ns\n", 1LL * (ts_cur.tv_sec - ctx->ts_start.tv_sec) * 1000000000
					+ (ts_cur.tv_nsec - ctx->ts_start.tv_nsec));

	ctx->is_completed = 1;
	if (spdk_nvme_cpl_is_error(completion)) {
		spdk_nvme_qpair_print_completion(ctx->ns_entry->qpair,
						 (struct spdk_nvme_cpl *)completion);
		fprintf(stderr, "I/O error status: %s\n",
				spdk_nvme_cpl_get_status_string(&completion->status));
		fprintf(stderr, "Write I/O failed, aborting run\n");
		ctx->is_completed = 2;
	}
	spdk_free(ctx->buf);
}

static void
do_rw(void)
{
	struct ns_entry		*ns_entry;
	struct nvme_rw_ctx	ctx;
	int			rc;

	ns_entry = g_namespaces;
	if (ns_entry != NULL) {
		ns_entry->qpair = spdk_nvme_ctrlr_alloc_io_qpair(ns_entry->ctrlr, NULL, 0);
		if (ns_entry->qpair == NULL) {
			fprintf(stderr, "ERROR: spdk_nvme_ctrlr_alloc_io_qpair() failed\n");
			return;
		}

		g_sectsize = spdk_nvme_ns_get_sector_size(ns_entry->ns);
		fprintf(stderr, "Sector size %u\n", g_sectsize);
		ctx.buf = spdk_zmalloc(g_sectsize * g_lbacnt, 0x1000, &ctx.phys_addr,
				SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);
		printf("Buffer Phsysical Address: %lx\n", ctx.phys_addr);
		if (ctx.buf == NULL) {
			fprintf(stderr, "ERROR: write buffer allocation failed\n");
			return;
		}
		ctx.is_completed = 0;
		ctx.ns_entry = ns_entry;

		if (g_write) {
			int rcnt = read(g_fd, ctx.buf, g_sectsize * g_lbacnt);
			fprintf(stderr, "%d bytes\n", rcnt);

			clock_gettime(CLOCK_MONOTONIC, &ctx.ts_start);
			rc = spdk_nvme_ns_cmd_write(ns_entry->ns, ns_entry->qpair, ctx.buf,
					g_startlba, g_lbacnt, write_complete, &ctx, 0);
			if (rc != 0) {
				fprintf(stderr, "starting write I/O failed\n");
				exit(1);
			}
		} else {
			clock_gettime(CLOCK_MONOTONIC, &ctx.ts_start);
			rc = spdk_nvme_ns_cmd_read(ns_entry->ns, ns_entry->qpair, ctx.buf,
					g_startlba, g_lbacnt, read_complete, &ctx, 0);
			if (rc != 0) {
				fprintf(stderr, "starting read I/O failed\n");
				exit(1);
			}
		}

		while (!ctx.is_completed)
			spdk_nvme_qpair_process_completions(ns_entry->qpair, 0);

		spdk_nvme_ctrlr_free_io_qpair(ns_entry->qpair);
		ns_entry = ns_entry->next;
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
	fprintf(stderr, " -r         read (default)\n");
	fprintf(stderr, " -f FILE    input/output file (use stdin/stdout by default)\n");
	fprintf(stderr, " -s LBA     start lba (default 0)\n");
	fprintf(stderr, " -c NLBA    number of blocks (default 1)\n");
}

static int
parse_args(int argc, char **argv)
{
	int op;

	while ((op = getopt(argc, argv, "Vwrf:s:c:")) != -1) {
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
		case 'f':
			g_fpath = optarg;
			break;
		case 's':
			g_startlba = atoi(optarg);
			break;
		case 'c':
			g_lbacnt = atoi(optarg);
			if (g_lbacnt <= 0) {
				fprintf(stderr, "wrong lbacnt\n");
				return 1;
			}
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
	if (rc != 0) {
		return rc;
	}

	spdk_env_opts_init(&opts);
	opts.name = "my_rw";
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

	if (g_write) {
		if (g_fpath) {
			g_fd = open(g_fpath, O_RDONLY);
			if (g_fd == -1) {
				perror("open");
				return 1;
			}
		} else {
			g_fd = STDIN_FILENO;
		}
	} else {
		if (g_fpath) {
			g_fd = open(g_fpath, O_WRONLY | O_CREAT | O_TRUNC, 0644);
			if (g_fd == -1) {
				perror("open");
				return 1;
			}
		} else {
			g_fd = STDOUT_FILENO;
		}
	}

	fprintf(stderr, "Initialization complete.\n");
	do_rw();
	cleanup();
	if (g_vmd)
		spdk_vmd_fini();

	return 0;
}
