/*-
 *   BSD LICENSE
 *
 *   Copyright (c) Intel Corporation.
 *   All rights reserved.
 *
 *   Redistribution and use in source and binary forms, with or without
 *   modification, are permitted provided that the following conditions
 *   are met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in
 *       the documentation and/or other materials provided with the
 *       distribution.
 *     * Neither the name of Intel Corporation nor the names of its
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 *   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 *   "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 *   LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 *   A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 *   OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 *   SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 *   LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 *   DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 *   THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 *   (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 *   OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#include "spdk/stdinc.h"

#include "spdk/nvme.h"
#include "spdk/vmd.h"
#include "spdk/env.h"

#include <unistd.h>
#include <fcntl.h>

#define SPDK_NVME_OPC_COSMOS_WRITE 0x81
#define SPDK_NVME_OPC_COSMOS_READ  0x82

#define COSMOS_OBJ_SIZE  (4 * 1024 * 1024)
#define COSMOS_OBJ_ALIGN (0x10000)

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
static uint32_t g_key = 0;

static unsigned int g_sectsize;

static struct spdk_nvme_cmd g_cmd = {
	.opc = SPDK_NVME_OPC_COSMOS_READ,
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

	printf("  Namespace ID: %d size: %juGB\n", spdk_nvme_ns_get_id(ns),
	       spdk_nvme_ns_get_size(ns) / 1000000000);
}

struct hello_world_sequence {
	struct ns_entry	*ns_entry;
	char		*buf;
	uint64_t	phys_addr;
	int		is_completed;
};

static void
read_complete(void *arg, const struct spdk_nvme_cpl *completion)
{
	struct hello_world_sequence *sequence = arg;
	int wcnt;

	sequence->is_completed = 1;
	if (spdk_nvme_cpl_is_error(completion)) {
		spdk_nvme_qpair_print_completion(sequence->ns_entry->qpair, (struct spdk_nvme_cpl *)completion);
		fprintf(stderr, "I/O error status: %s\n", spdk_nvme_cpl_get_status_string(&completion->status));
		fprintf(stderr, "Read I/O failed, aborting run\n");
		sequence->is_completed = 2;
	}

	wcnt = write(g_fd, sequence->buf, COSMOS_OBJ_SIZE);
	printf("\n\nread %d bytes\n", wcnt);

	spdk_free(sequence->buf);
}

static void
write_complete(void *arg, const struct spdk_nvme_cpl *completion)
{
	struct hello_world_sequence *sequence = arg;

	sequence->is_completed = 1;
	if (spdk_nvme_cpl_is_error(completion)) {
		spdk_nvme_qpair_print_completion(sequence->ns_entry->qpair, (struct spdk_nvme_cpl *)completion);
		fprintf(stderr, "I/O error status: %s\n", spdk_nvme_cpl_get_status_string(&completion->status));
		fprintf(stderr, "Write I/O failed, aborting run\n");
		sequence->is_completed = 2;
	}
	spdk_free(sequence->buf);
}

static void
do_rw(void)
{
	struct ns_entry			*ns_entry;
	struct hello_world_sequence	sequence;
	int				rc;

	ns_entry = g_namespaces;
	if (ns_entry != NULL) {
		ns_entry->qpair = spdk_nvme_ctrlr_alloc_io_qpair(ns_entry->ctrlr, NULL, 0);
		if (ns_entry->qpair == NULL) {
			printf("ERROR: spdk_nvme_ctrlr_alloc_io_qpair() failed\n");
			return;
		}

		g_sectsize = spdk_nvme_ns_get_sector_size(ns_entry->ns);
		sequence.buf = spdk_zmalloc(COSMOS_OBJ_SIZE, COSMOS_OBJ_ALIGN, &sequence.phys_addr,
						SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);
		if (sequence.buf == NULL) {
			printf("ERROR: write buffer allocation failed\n");
			return;
		}
		sequence.is_completed = 0;
		sequence.ns_entry = ns_entry;

		g_cmd.rsvd2 = sequence.phys_addr & ((1ULL << 32) - 1);
		g_cmd.rsvd3 = sequence.phys_addr >> 32;
		g_cmd.cdw10 = g_key;

		if (g_write) {
			int rcnt = read(g_fd, sequence.buf, COSMOS_OBJ_SIZE);
			printf("\n\nwrite %d bytes\n", rcnt);

			g_cmd.opc = SPDK_NVME_OPC_COSMOS_WRITE;

			rc = spdk_nvme_ctrlr_io_cmd_raw_no_payload_build(ns_entry->ns, ns_entry->qpair, &g_cmd,
					write_complete, &sequence);
			if (rc != 0) {
				fprintf(stderr, "starting write I/O failed\n");
				exit(1);
			}
		} else {
			g_cmd.opc = SPDK_NVME_OPC_COSMOS_READ;

			rc = spdk_nvme_ctrlr_io_cmd_raw_no_payload_build(ns_entry->ns, ns_entry->qpair, &g_cmd,
					read_complete, &sequence);
			if (rc != 0) {
				fprintf(stderr, "starting read I/O failed\n");
				exit(1);
			}
		}

		while (!sequence.is_completed)
			spdk_nvme_qpair_process_completions(ns_entry->qpair, 0);

		spdk_nvme_ctrlr_free_io_qpair(ns_entry->qpair);
		ns_entry = ns_entry->next;
	}
}

static bool
probe_cb(void *cb_ctx, const struct spdk_nvme_transport_id *trid,
	 struct spdk_nvme_ctrlr_opts *opts)
{
	printf("Attaching to %s\n", trid->traddr);

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

	printf("Attached to %s\n", trid->traddr);

	cdata = spdk_nvme_ctrlr_get_data(ctrlr);

	snprintf(entry->name, sizeof(entry->name), "%-20.20s (%-20.20s)", cdata->mn, cdata->sn);

	entry->ctrlr = ctrlr;
	entry->next = g_controllers;
	g_controllers = entry;

	num_ns = spdk_nvme_ctrlr_get_num_ns(ctrlr);
	printf("Using controller %s with %d namespaces.\n", entry->name, num_ns);
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
	printf("%s [options]", program_name);
	printf("\n");
	printf("options:\n");
	printf(" -V         enumerate VMD\n");
	printf(" -w         write\n");
	printf(" -r         read\n");
	printf(" -f FILE    input/output file\n");
	printf(" -k KEY     key\n");
}

static int
parse_args(int argc, char **argv)
{
	int op;

	while ((op = getopt(argc, argv, "Vwrf:k:")) != -1) {
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
		case 'k':
			g_key = atoi(optarg);
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

	printf("Initializing NVMe Controllers\n");

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
		} else {
			g_fd = STDOUT_FILENO;
			if (g_fd == -1) {
				perror("open");
				return 1;
			}
		}
	}

	printf("Initialization complete.\n");
	do_rw();
	cleanup();
	if (g_vmd)
		spdk_vmd_fini();

	return 0;
}
