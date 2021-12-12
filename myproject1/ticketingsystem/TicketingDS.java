package ticketingsystem;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

public class TicketingDS implements TicketingSystem {

    /* Key parameters */

    int route_num;
    int coach_num;
    int seat_num;
    int station_num;
    int thread_num;

    /* Memory Padding for Better Cache Access */

    private static final int padding = 4;

    /* Invalid intervals stored in bitmap format */

    static AtomicIntegerArray[] seat_record;
    int record_size;

    /* Structures to store information of sold tickets */

    static AtomicLong tid = new AtomicLong(0);
    volatile String[] sold_name;
    static AtomicIntegerArray sold_hash;

    /* Array to record the local TID for every thread */

    long[][] thread_tid;

    /* Cache to record a minimun interval with no available tickets */

    AtomicLongArray empty_cache[];

    /* Time-stamp for cache updating */

    AtomicLong action = new AtomicLong(0);

    /* Parameters for analysis of cache structure */

    AtomicInteger benefit = new AtomicInteger(0);
    AtomicInteger failure = new AtomicInteger(0);

    public int getFailedBuying () {
        return failure.get();
    }

    public int getCacheBenefit () {
        return benefit.get();
    }

    /* Initialize the cache structure */

    private void initCache () {
        empty_cache = new AtomicLongArray[route_num];
        for (int i = 0; i < route_num; i++) {
            empty_cache[i] = new AtomicLongArray(8);
            empty_cache[i].set(0, 0x00000000ffffffffL);
        }
    }

    /* Return True if the record in cache is a subset of the interval to search */

    private boolean readCache (int index, int interval) {
        int empty_interval = (int) (empty_cache[index].get(0) & 0xffffffffL);
        if ((empty_interval & interval) == empty_interval) benefit.getAndIncrement();
        return (empty_interval & interval) == empty_interval;
    }

    /* Update a more precise range for cache if possible */

    private boolean updateCache (int index, int interval, long stamp) {
        AtomicLongArray cache = empty_cache[index];
        long cache_info = cache.get(0);
        int old_interval = (int) (cache_info & 0x00000000ffffffffL);
        long old_stamp = cache_info >>> 32;
        if (Integer.bitCount(old_interval) <= Integer.bitCount(interval) || stamp <= old_stamp) return true;
        long new_info = stamp << 32 | (long) interval;
        return cache.compareAndSet(0, cache_info, new_info);
    }

    /* Invalidate the cache if a refunded interval intersects with the recorded */

    private void flushCache (int index, int interval) {
        AtomicLongArray cache = empty_cache[index];
        long cache_info = cache.get(0);
        int old_interval = (int) (cache_info & 0x00000000ffffffffL);
        if ((old_interval & interval) != 0) {
            long new_stamp = action.incrementAndGet();
            long new_info = new_stamp << 32 | 0x00000000ffffffffL;
            cache.set(0, new_info);
        }
    }

    /* Strict version of updating. Keep trying until the time-stamp / interval is invalid or success */

    private void updateCacheStrict (int index, int interval, long stamp) {
        while (!updateCache(index, interval, stamp)) ;
    }

    /* Strict version of flushing. Keep trying until reset the record with a newest time-stamp */

    private void flushCacheStrict (int index, int interval) {
        AtomicLongArray cache = empty_cache[index];
        while (true) {
            long cache_info = cache.get(0);
            int old_interval = (int) (cache_info & 0x00000000ffffffffL);
            if ((old_interval & interval) == 0) return;
            long new_stamp = action.incrementAndGet();
            long new_info = new_stamp << 32 | 0x00000000ffffffffL;
            if (cache.compareAndSet(0, cache_info, new_info)) return;
        }
    }

    /* Simply get a new TID by global TID + 1 */

    private long getId() {
        return tid.getAndIncrement();
    }

    /* Get a number of TIDs for a thread every time to avoid competition */

    private long getIdLocal () {

        /* Group by the last 7 bits (128 groups in all) */

        int thread_id = (int)(Thread.currentThread().getId()) & 0x7f;

        /* Allocate 256 more TID if the former have been used up */

        if ((thread_tid[thread_id][0] & 0x7f) == 0)
            thread_tid[thread_id][0] = tid.getAndAdd(128);

        return thread_tid[thread_id][0]++;
    }

    /* Get a compressed record of sold ticket to reduce memory usage */

    private int getHash(Ticket t) {
        int index = (t.coach - 1) * seat_num + t.seat - 1;
        return t.arrival | t.departure << 6 | t.route << 12 | index << 18;
    }

    /* Generate a new Ticket object */

    private Ticket newTicket(String passenger, int route, int index, int departure, int arrival) {
        long id = getIdLocal();
        int coach = index / seat_num + 1;
        int seat = index % seat_num + 1;
        Ticket t = new Ticket();
        t.tid = id;
        t.passenger = passenger;
        t.route = route;
        t.coach = coach;
        t.seat = seat;
        t.departure = departure;
        t.arrival = arrival;
        sold_hash.set((int) id, getHash(t));
        sold_name[(int) id] = passenger;
        return t;
    }

    /* Get the interval in bitmap format */

    private int interval(int src, int dst) {
        int src_bit = 0xffffffff << (src - 1);
        int dst_bit = 0xffffffff >>> (33 - dst);
        return src_bit & dst_bit;
    }

    /* Initialize necessary structures */

    private void initDS() {
        if (station_num > Integer.SIZE)
            throw new IllegalArgumentException();
        record_size = coach_num * seat_num;
        seat_record = new AtomicIntegerArray[route_num];
        for (int i = 0; i < route_num; i++) {
            int length = record_size << padding;
            seat_record[i] = new AtomicIntegerArray(length);
            for (int j = 0; j < length; j++)
                seat_record[i].set(j, 0);
        }
        thread_tid = new long[128][8];
        for (int i = 0; i < 128; i++) {
            thread_tid[i][0] = 0;
        }
        int max_tid = thread_num * 120000;
        sold_hash = new AtomicIntegerArray(max_tid);
        sold_name = new String[max_tid];
        for (int i = 0; i < max_tid; i++) {
            sold_name[i] = null;
            sold_hash.set(i, 0xffffffff);
        }
        initCache();
    }

    /* Default Constructor */

    public TicketingDS() {
        this.route_num = 5;
        this.coach_num = 8;
        this.seat_num = 100;
        this.station_num = 10;
        this.thread_num = 16;
        initDS();
    }

    /* Constructor with input parameters */

    public TicketingDS(int route_num, int coach_num, int seat_num, int station_num, int thread_num) {
        this.route_num = route_num;
        this.coach_num = coach_num;
        this.seat_num = seat_num;
        this.station_num = station_num;
        this.thread_num = thread_num;
        initDS();
    }

    /* Interface of Buying A Ticket */

    public Ticket buyTicket(String passenger, int route, int departure, int arrival) {

        /* Return NULL if the interval to buying is predicted to be unavailable */

        int interval = interval(departure, arrival);
        // if (readCache(route - 1, interval)) return null;

        AtomicIntegerArray route_record = seat_record[route - 1];

        /* Select a random start to avoid competition. Hashcode seems faster than Random method */

        // int end = random.nextInt(record_size);
        // int end = (passenger.hashCode() & 0x1ff);
        int end = (passenger.hashCode() & 0x7fffffff) % record_size;
        int index = end;

        /* For searching, the time-stamp has to be allocated before start to guarantee linearizable */

        // long stamp = action.incrementAndGet();

        do {
            /* Keep searching until succeed */
            while (true) {
                /* Get correct index after padding */
                int position = index << padding;
                /* Give up current seat */
                int record = route_record.get(position);
                if ((record & interval) != 0) break;
                int new_record = record | interval;
                boolean res = route_record.compareAndSet(position, record, new_record);
                /* Another thread may try writing the same seat. Check again if CAS failed */
                if (res)
                    return newTicket(passenger, route, index, departure, arrival);
            }

            index++;
            if (index == record_size) index = 0;

        } while (index != end);

        /* Update cache with the sold-out interval */

        // updateCache(route - 1, interval, stamp);
        // updateCacheStrict(route - 1, interval, stamp);

        // failure.getAndIncrement();

        return null;
    }

    /* Interface of Inquiry Remaining Tickets */

    public int inquiry(int route, int departure, int arrival) {

        /* Return 0 if the interval to buying is predicted to be unavailable */

        int interval = interval(departure, arrival);
        // if (readCache(route - 1, interval)) return 0;

        int cnt = 0;
        AtomicIntegerArray route_record = seat_record[route - 1];

        /* Traverse all seats to count available seats */

        for (int i = 0; i < record_size; i++) {
            /* Shift to get correct index after padding */
            int record = route_record.get(i << padding);
            if ((record & interval) == 0) cnt++;
        }

        /* Update the cache with the sold-out interval */

        // if (cnt == 0) updateCache(route - 1, interval);

        // if (cnt == 0) failure.getAndIncrement();

        return cnt;
    }

    /* Interface of Refunding A Ticket */

    public boolean refundTicket(Ticket ticket) {

        int id = (int) ticket.tid;

        /* Refunding an unsold ticket will fail */

        if (sold_name[id] == null) return false;

        /* Refunding a ticket with wrong passenger name will fail */

        if (!Objects.equals(ticket.passenger, sold_name[id]))
            return false;

        /* Refunding a ticket with wrong information (except name) will fail */

        int t_hash = getHash(ticket);
        if (!sold_hash.compareAndSet(id, t_hash, 0xffffffff))
            return false;

        int interval = ~interval(ticket.departure, ticket.arrival);
        int index = (ticket.coach - 1) * seat_num + ticket.seat - 1;
        AtomicIntegerArray record = seat_record[ticket.route - 1];

        /* Get correct index after padding */

        int position = index << padding;

        /* Reset the sold bitmap */

        while (true) {
            int old_record = record.get(position);
            int new_record = old_record & interval;
            if (record.compareAndSet(position, old_record, new_record)) break;
        }

        /* Flush the cache with the refunded interval */

        // flushCache(ticket.route - 1, ~interval);
        // flushCacheStrict(ticket.route - 1, ~interval);

        return true;
    }

    /* Unrealized */

    public boolean buyTicketReplay(Ticket ticket) {
        return true;
    }

    public boolean refundTicketReplay(Ticket ticket) {
        return true;
    }

}
