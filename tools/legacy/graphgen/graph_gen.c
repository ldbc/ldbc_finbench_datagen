#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdint.h>
#include <assert.h>
#include <string.h>
#include <sys/time.h>
#include <omp.h>

#define LINE printf("LINE: %d\n", __LINE__)

typedef struct edge_pair_s {
    uint32_t v0;
    uint32_t v1;
} edge_pair_t;

typedef enum gen_type_s {
    type_kron,
    type_rmat,
    type_erdos
} gen_type_t;

inline double get_time()
{
    struct timeval time;
    gettimeofday(&time, NULL);
    return (double)time.tv_sec + (double)time.tv_usec * 1e-6;
}

/* xorshift128plus 1.12ns/64bit */
inline uint64_t my_rand(uint64_t* seed) {
    uint64_t x = seed[0];
    uint64_t const y = seed[1];
    seed[0] = y;
    x ^= x << 23;
    x ^= x >> 17;
    x ^= y ^ (y >> 26);
    seed[1] = x;
    return x + y;
}

#define RMAT_A    (0.57)
#define RMAT_B    (0.19)
#define RMAT_C    (0.19)
#define RMAT_D    (0.05)
#define RMAT_BD   (0.24)
#define RMAT_BDC  (0.43)
#define RMAT_ABCD (1.00)

#define UINT64_B    (3504881374004814806UL)
#define UINT64_BD   (4427218577690292387UL)
#define UINT64_BDC  (7932099951695107194UL)
#define UINT64_ABCD (18446744073709551615UL)
#define MAX_UINT64  (18446744073709551615UL)

inline void choose_partition_kron(uint64_t*seed, uint64_t* u, uint64_t* v, uint64_t step) 
{
    uint64_t p;
    p = my_rand(seed);
    if (p < UINT64_BD)
        *v = *v + step;
    if (p > UINT64_B && p < UINT64_BDC)
        *u = *u + step;
}

inline void choose_partition_rmat(uint64_t* seed, uint64_t* u, uint64_t* v, uint64_t step, 
        double a, double b, double c, double d) 
{
    uint64_t p;
    p = my_rand(seed);
    if (p < (b + d) * MAX_UINT64)
        *v = *v + step;
    if (p > b * MAX_UINT64 && p < (b + c + d) * MAX_UINT64)
        *u = *u + step;
}

inline void vary_para(uint64_t* seed, double* a, double* b, double* c, double* d) 
{
    double s;
    *a = *a * ( 0.95 + 0.1 * my_rand(seed) / MAX_UINT64);
    *b = *b * ( 0.95 + 0.1 * my_rand(seed) / MAX_UINT64);
    *c = *c * ( 0.95 + 0.1 * my_rand(seed) / MAX_UINT64);
    *d = *d * ( 0.95 + 0.1 * my_rand(seed) / MAX_UINT64);
    s = *a + *b + *c + *d; 
    *a = *a / s;
    *b = *b / s;
    *c = *c / s;
    *d = *d / s;
}

inline uint64_t permutation(uint64_t x, uint64_t n)
{
    return ((x+495211) * 179428261 % n);
}

edge_pair_t* gen_edgelist(uint64_t seed_arg, uint64_t num_vtx, uint64_t num_edge, gen_type_t gen_type)
{

    // gen edge
    edge_pair_t* edgelist = (edge_pair_t*) malloc (num_edge * sizeof(edge_pair_t));

#pragma omp parallel
    {
        uint64_t seed[2];
        seed[0] = seed_arg * 7 + omp_get_thread_num() + 132;
        seed[1] = seed_arg * 31 + omp_get_thread_num() * 10 + 11;
#pragma omp for
        for (uint64_t i=0; i<num_edge; i++) {
            uint64_t v0, v1;

            switch (gen_type) {
                case type_kron:
                    {
                        uint64_t step;
                        v0 = 0;
                        v1 = 0;
                        step = num_vtx >> 1;
                        while (step >= 1) {
                            choose_partition_kron(seed, &v0, &v1, step);
                            step = step >> 1;
                        }
                    }
                    break;
                case type_rmat:
                    {
                        uint64_t step;

                        double a = 0.57;
                        double b = 0.19;
                        double c = 0.19;
                        double d = 0.05;
                        v0 = 0;
                        v1 = 0;
                        step = num_vtx >> 1;
                        while (step >= 1) {
                            choose_partition_rmat(seed, &v0, &v1, step, a, b, c, d);
                            vary_para(seed, &a, &b, &c, &d);
                            step = step >> 1;
                        }
                    }
                    break;
                case type_erdos:
                    {
                        v0 = my_rand(seed) % num_vtx;
                        v1 = my_rand(seed) % num_vtx;
                    }
                    break;
                default:
                    printf("ERROR gen_type!\n");
                    break;
            }
            v0 = permutation(v0, num_vtx);
            v1 = permutation(v1, num_vtx);
            edgelist[i].v0 = v0;
            edgelist[i].v1 = v1;
            //printf("[%d] %d %d  seed: %d %d\n", omp_get_thread_num(), v0, v1, seed[0], seed[1]);
        }
    }
    return edgelist;
}

char* get_type_name(gen_type_t t)
{
    char *name = (char*)"none";
    if (t == type_kron)
        name = (char*)"kron";
    else if (t == type_rmat)
        name = (char*)"rmat";
    else if (t == type_erdos)
        name = (char*)"erdos";
    return name;
}

int main(int argc, char** argv)
{
    int scale;
    int edge_factor;
    edge_pair_t* edgelist;
    uint64_t num_vtx, num_edge;
    int seed_arg = 0;
    bool gen_vertex = false;
    gen_type_t gen_type;

    scale = 18;
    edge_factor = 16;
    gen_type = type_kron;

    int op;
    while ((op = getopt (argc, argv, "hs:e:t:x:v:")) != -1) {
        switch (op)
        {
            case 's':
                {
                    scale = atoi(optarg);
                }
                break;
            case 'e':
                {
                    edge_factor = atoi(optarg);
                }
                break;
            case 'v':
                {
                    gen_vertex = atoi(optarg);
                }
                break;
            case 't':
                {
                    if (strcmp(optarg, "kron") == 0)
                        gen_type = type_kron;
                    else if (strcmp(optarg, "rmat") == 0)
                        gen_type = type_rmat;
                    else if (strcmp(optarg, "erdos") == 0)
                        gen_type = type_erdos;
                    else
                        printf("Unsupport type!\n");
                }
                break;
            case 'x':
                {
                    seed_arg = atoi(optarg);
                }
                break;
            case 'h':
            case '?':
                {
                    printf("Usage: ./graph_gen -s [scale] -e [edge_factor] -t [gen_type] -x [seed]\n");
                    printf("    Supported gen_type include kron/rmat/erdos\n");
                }
                abort();
                break;
            default:
                {
                    printf("Unsupport arg: %c\n", op);
                }
        }
    }

    printf("ARG:\n");
    printf("  scale: %d\n", scale);
    printf("  edge_factor: %d\n", edge_factor);
    printf("  gen_type: %s\n", get_type_name(gen_type));

    num_vtx = (1L<<scale);
    num_edge = num_vtx * edge_factor;


    {
        double cost = -1 * get_time();
        edgelist = NULL;
        edgelist = gen_edgelist(seed_arg, num_vtx, num_edge, gen_type);
        assert(edgelist != NULL);
        cost += get_time();
        printf("gen_edgelist cost: %.3lf\n", cost);
    }
    {
        double cost = -1 * get_time();
        printf("  num_vtx: %ld\n", num_vtx);
        printf("  num_edge: %ld\n", num_edge);
        char filename[64];

        if (gen_vertex) {
            sprintf(filename, "s%d.%s.vertex.txt", scale, get_type_name(gen_type));
            FILE* fp = fopen(filename, "w");
            for (uint64_t i=0; i<num_vtx; i++) {
                fprintf(fp, "%ld\n", i);
            }
            printf("  Write vertex to file: %s\n", filename);
        }
#if 1
        sprintf(filename, "s%d.%s.edge.txt.%d", scale, get_type_name(gen_type), seed_arg);
        FILE* fp = fopen(filename, "w");
        for (uint64_t i=0; i<num_edge; i++) {
            fprintf(fp, "%ld,%ld\n", (uint64_t)edgelist[i].v0, (uint64_t)edgelist[i].v1);
        }
        printf("  Write edge to file: %s\n", filename);
#else
        sprintf(filename, "s%d.%s.edgelist.%d", scale, get_type_name(gen_type), seed_arg);
        FILE* fp = fopen(filename, "wb");
        size_t sz = fwrite(edgelist, sizeof(edgelist), num_edge, fp);
        assert(sz == num_edge);
        printf("  Write data to file: %s\n", filename);
#endif
        cost += get_time();
        printf("write cost: %.3lf\n", cost);
    }
    return 0;
}