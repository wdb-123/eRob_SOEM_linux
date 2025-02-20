/* 
 * This program is an EtherCAT master implementation that initializes and configures EtherCAT slaves,
 * manages their states, and handles real-time data exchange. It includes functions for setting up 
 * PDO mappings, synchronizing time with the distributed clock, and controlling servomotors in 
 * various operational modes. The program also features multi-threading for real-time processing 
 * and monitoring of the EtherCAT network.
 */
//#include <QCoreApplication>


#include <stdio.h>
#include <string.h>
#include "ethercat.h"
#include <iostream>
#include <inttypes.h>
#include <time.h>
#include <unistd.h>
#include <sys/mman.h>

#include <sys/time.h>
#include <pthread.h>
#include <math.h>

#include <chrono>
#include <ctime>

#include <iostream>
#include <cstdint>

#include <sched.h>
#include <climits>  // 添加这行来获取 LONG_MAX 定义

// Global variables for EtherCAT communication
char IOmap[4096]; // I/O mapping for EtherCAT
int expectedWKC; // Expected Work Counter
boolean needlf; // Flag to indicate if a line feed is needed
volatile int wkc; // Work Counter (volatile to ensure it is updated correctly in multi-threaded context)
boolean inOP; // Flag to indicate if the system is in operational state
uint8 currentgroup = 0; // Current group for EtherCAT communication
int dorun = 0; // Flag to indicate if the thread should run
bool start_ecatthread_thread; // Flag to start the EtherCAT thread
int ctime_thread; // Cycle time for the EtherCAT thread

int64 toff, gl_delta; // Time offset and global delta for synchronization

// Function prototypes for EtherCAT thread functions
OSAL_THREAD_FUNC ecatcheck(void *ptr); // Function to check the state of EtherCAT slaves
OSAL_THREAD_FUNC_RT ecatthread(void *ptr); // Real-time EtherCAT thread function

// Thread handles for the EtherCAT threads
OSAL_THREAD_HANDLE thread1; // Handle for the EtherCAT check thread
OSAL_THREAD_HANDLE thread2; // Handle for the real-time EtherCAT thread

// Function to synchronize time with the EtherCAT distributed clock
void ec_sync(int64 reftime, int64 cycletime, int64 *offsettime);
// Function to add nanoseconds to a timespec structure
void add_timespec(struct timespec *ts, int64 addtime);

// Define constants for stack size and timing
#define stack64k (64 * 1024) // Stack size for threads
#define NSEC_PER_SEC 1000000000   // Number of nanoseconds in one second
#define EC_TIMEOUTMON 5000        // Timeout for monitoring in microseconds
#define MAX_VELOCITY 30000        // Maximum velocity
#define MAX_ACCELERATION 50000    // Maximum acceleration

// Conversion units for the servomotor
float Cnt_to_deg = 0.000686645; // Conversion factor from counts to degrees
int8_t SLAVE_ID; // Slave ID for EtherCAT communication

// Structure for RXPDO (Control data sent to slave)
typedef struct {
    uint16_t controlword;      // 0x6040:0, 16 bits
    int32_t target_velocity;   // 0x60FF:0, 32 bits
    uint8_t mode_of_operation; // 0x6060:0, 8 bits
    uint8_t padding;          // 8 bits padding for alignment
} __attribute__((__packed__)) rxpdo_t;

// Structure for TXPDO (Status data received from slave)
typedef struct {
    uint16_t statusword;      // 0x6041:0, 16 bits
    int32_t actual_position;  // 0x6064:0, 32 bits
    int32_t actual_velocity;  // 0x606C:0, 32 bits
    int16_t actual_torque;    // 0x6077:0, 16 bits
} __attribute__((__packed__)) txpdo_t;

// Global variables
volatile int target_position = 0;
pthread_mutex_t target_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t target_position_cond = PTHREAD_COND_INITIALIZER;
bool target_updated = false;
int32_t received_target = 0;

rxpdo_t rxpdo;  // Global variable, used for sending data to slaves
txpdo_t txpdo;  // Global variable, used for receiving data from slaves

struct MotorStatus {
    bool is_operational;
    uint16_t status_word;
    int32_t actual_position;
    int32_t actual_velocity;
    int16_t actual_torque;
} motor_status;

// Function to update motor status information
void update_motor_status(int slave_id) {
    // Update status information from TXPDO
    motor_status.status_word = txpdo.statusword;
    motor_status.actual_position = txpdo.actual_position;
    motor_status.actual_velocity = txpdo.actual_velocity;
    motor_status.actual_torque = txpdo.actual_torque;
    
    // Check status word to determine if motor is operational
    // Bits 0-3 should be 0111 for enabled and ready state
    motor_status.is_operational = (txpdo.statusword & 0x0F) == 0x07;
}

//##################################################################################################
// Function: Set the CPU affinity for a thread
void set_thread_affinity(pthread_t thread, int cpu_core) {
    cpu_set_t cpuset; // CPU set to specify which CPUs the thread can run on
    CPU_ZERO(&cpuset); // Clear the CPU set
    CPU_SET(cpu_core, &cpuset); // Add the specified CPU core to the set

    // Set the thread's CPU affinity
    int result = pthread_setaffinity_np(thread, sizeof(cpu_set_t), &cpuset);
    if (result != 0) {
        printf("Unable to set CPU affinity for thread %d\n", cpu_core); // Error message if setting fails
    } else {
        printf("Thread successfully bound to CPU %d\n", cpu_core); // Confirmation message if successful
    }
}

//##################################################################################################
// Function prototype for the EtherCAT test function
int erob_test();

uint16_t data_R;

int erob_test() {
    int rdl; // Variable to hold read data length
    SLAVE_ID = 1; // Set the slave ID to 1
    int i, j, oloop, iloop, chk; // Loop control variables

    // 1. Call ec_config_init() to move from INIT to PRE-OP state.
    printf("__________STEP 1___________________\n");
    // Initialize EtherCAT master on the specified network interface
    if (ec_init("enp6s0") <= 0) {
        printf("Error: Could not initialize EtherCAT master!\n");
        printf("No socket connection on Ethernet port. Execute as root.\n");
        printf("___________________________________________\n");
        return -1; // Return error if initialization fails
    }
    printf("EtherCAT master initialized successfully.\n");
    printf("___________________________________________\n");

    // Search for EtherCAT slaves on the network
    if (ec_config_init(FALSE) <= 0) {
        printf("Error: Cannot find EtherCAT slaves!\n");
        printf("___________________________________________\n");
        ec_close(); // Close the EtherCAT connection
        return -1; // Return error if no slaves are found
    }
    printf("%d slaves found and configured.\n", ec_slavecount); // Print the number of slaves found
    printf("___________________________________________\n");

    // 2. Change to pre-operational state to configure the PDO registers
    printf("__________STEP 2___________________\n");

    // Check if the slave is ready to map
    ec_readstate(); // Read the state of the slaves

    for(int i = 1; i <= ec_slavecount; i++) { // Loop through each slave
        if(ec_slave[i].state != EC_STATE_PRE_OP) { // If the slave is not in PRE-OP state
            // Print the current state and status code of the slave
            printf("Slave %d State=0x%2.2x StatusCode=0x%4.4x : %s\n",
                   i, ec_slave[i].state, ec_slave[i].ALstatuscode, ec_ALstatuscode2string(ec_slave[i].ALstatuscode));
            printf("\nRequest init state for slave %d\n", i); // Request to change the state to INIT
            ec_slave[i].state = EC_STATE_INIT; // Set the slave state to INIT
            printf("___________________________________________\n");
        } else { // If the slave is in PRE-OP state
            ec_slave[0].state = EC_STATE_PRE_OP; // Set the first slave to PRE-OP state
            /* Request EC_STATE_PRE_OP state for all slaves */
            ec_writestate(0); // Write the state change to the slave
            /* Wait for all slaves to reach the PRE-OP state */
            if ((ec_statecheck(0, EC_STATE_PRE_OP,  3 * EC_TIMEOUTSTATE)) == EC_STATE_PRE_OP) {
                printf("State changed to EC_STATE_PRE_OP: %d \n", EC_STATE_PRE_OP);
                printf("___________________________________________\n");
            } else {
                printf("State EC_STATE_PRE_OP cannot be changed in step 2\n");
                return -1; // Return error if state change fails
            }
        }
    }

//##################################################################################################
    //3.- Map RXPOD
    printf("__________STEP 3___________________\n");

    // Modify PDO mapping configuration
    int retval = 0;
    uint16 map_1c12;
    uint8 zero_map = 0;
    uint32 map_object;
    uint16 clear_val = 0x0000;

    for(int i = 1; i <= ec_slavecount; i++) {
        // Clear RXPDO mapping
        retval += ec_SDOwrite(i, 0x1600, 0x00, FALSE, sizeof(zero_map), &zero_map, EC_TIMEOUTSAFE);
        
        // Control word (0x6040:0, 16 bits)
        map_object = 0x60400010;
        retval += ec_SDOwrite(i, 0x1600, 0x01, FALSE, sizeof(map_object), &map_object, EC_TIMEOUTSAFE);
        
        // Target velocity (0x60FF:0, 32 bits)
        map_object = 0x60FF0020;
        retval += ec_SDOwrite(i, 0x1600, 0x02, FALSE, sizeof(map_object), &map_object, EC_TIMEOUTSAFE);
        
        // Operation mode (0x6060:0, 8 bits)
        map_object = 0x60600008;
        retval += ec_SDOwrite(i, 0x1600, 0x03, FALSE, sizeof(map_object), &map_object, EC_TIMEOUTSAFE);
        
        // Padding (8 bits padding)
        map_object = 0x00000008;
        retval += ec_SDOwrite(i, 0x1600, 0x04, FALSE, sizeof(map_object), &map_object, EC_TIMEOUTSAFE);
        
        uint8 map_count = 4;  // Now there are 4 objects, including padding
        retval += ec_SDOwrite(i, 0x1600, 0x00, FALSE, sizeof(map_count), &map_count, EC_TIMEOUTSAFE);
        
        // Configure RXPDO allocation
        clear_val = 0x0000;
        retval += ec_SDOwrite(i, 0x1c12, 0x00, FALSE, sizeof(clear_val), &clear_val, EC_TIMEOUTSAFE);
        map_1c12 = 0x1600;
        retval += ec_SDOwrite(i, 0x1c12, 0x01, FALSE, sizeof(map_1c12), &map_1c12, EC_TIMEOUTSAFE);
        map_1c12 = 0x0001;
        retval += ec_SDOwrite(i, 0x1c12, 0x00, FALSE, sizeof(map_1c12), &map_1c12, EC_TIMEOUTSAFE);
    }

    printf("RXPDO mapping configuration result: %d\n", retval);
    if (retval < 0) {
        printf("RXPDO mapping failed\n");
        return -1;
    }

    printf("RXPOD Mapping set correctly.\n");
    printf("___________________________________________\n");

    //........................................................................................
    // Map TXPOD
    retval = 0;
    uint16 map_1c13;
    for(int i = 1; i <= ec_slavecount; i++) {
        // Clear TXPDO mapping
        clear_val = 0x0000;
        retval += ec_SDOwrite(i, 0x1A00, 0x00, FALSE, sizeof(clear_val), &clear_val, EC_TIMEOUTSAFE);

        // Status Word (0x6041:0, 16 bits)
        map_object = 0x60410010;
        retval += ec_SDOwrite(i, 0x1A00, 0x01, FALSE, sizeof(map_object), &map_object, EC_TIMEOUTSAFE);

        // Actual Position (0x6064:0, 32 bits)
        map_object = 0x60640020;
        retval += ec_SDOwrite(i, 0x1A00, 0x02, FALSE, sizeof(map_object), &map_object, EC_TIMEOUTSAFE);

        // Actual Velocity (0x606C:0, 32 bits)
        map_object = 0x606C0020;
        retval += ec_SDOwrite(i, 0x1A00, 0x03, FALSE, sizeof(map_object), &map_object, EC_TIMEOUTSAFE);

        // Actual Torque (0x6077:0, 16 bits)
        map_object = 0x60770010;
        retval += ec_SDOwrite(i, 0x1A00, 0x04, FALSE, sizeof(map_object), &map_object, EC_TIMEOUTSAFE);

        uint8 map_count = 4;  // Ensure mapping 4 objects
        retval += ec_SDOwrite(i, 0x1A00, 0x00, FALSE, sizeof(map_count), &map_count, EC_TIMEOUTSAFE);

        // Correctly configure TXPDO allocation
        clear_val = 0x0000;
        retval += ec_SDOwrite(i, 0x1C13, 0x00, FALSE, sizeof(clear_val), &clear_val, EC_TIMEOUTSAFE);
        map_1c13 = 0x1A00;
        retval += ec_SDOwrite(i, 0x1C13, 0x01, FALSE, sizeof(map_1c13), &map_1c13, EC_TIMEOUTSAFE);
        map_1c13 = 0x0001;
        retval += ec_SDOwrite(i, 0x1C13, 0x00, FALSE, sizeof(map_1c13), &map_1c13, EC_TIMEOUTSAFE);


    }

    printf("Slave %d TXPDO mapping configuration result: %d\n", SLAVE_ID, retval);

    if (retval < 0) {
        printf("TXPDO Mapping failed\n");
        printf("___________________________________________\n");
        return -1;
    }

    printf("TXPDO Mapping set successfully\n");
    printf("___________________________________________\n");

   //##################################################################################################

    //4.- Set ecx_context.manualstatechange = 1. Map PDOs for all slaves by calling ec_config_map().
   printf("__________STEP 4___________________\n");

   ecx_context.manualstatechange = 1; //Disable automatic state change
   osal_usleep(1e6); //Sleep for 1 second

    uint8 WA = 0; //Variable for write access
    uint8 my_RA = 0; //Variable for read access
    uint32 TIME_RA; //Variable for time read access

    // Print the information of the slaves found
    for (int i = 1; i <= ec_slavecount; i++) {
       // (void)ecx_FPWR(ecx_context.port, i, ECT_REG_DCSYNCACT, sizeof(WA), &WA, 5 * EC_TIMEOUTRET);
        printf("Name: %s\n", ec_slave[i].name); //Print the name of the slave
        printf("Slave %d: Type %d, Address 0x%02x, State Machine actual %d, required %d\n", 
               i, ec_slave[i].eep_id, ec_slave[i].configadr, ec_slave[i].state, EC_STATE_INIT);
        printf("___________________________________________\n");
        ecx_dcsync0(&ecx_context, i, TRUE, 1000000, 0);  //Synchronize the distributed clock for the slave
    }

    // Map the configured PDOs to the IOmap
    ec_config_map(&IOmap);

    printf("__________STEP 5___________________\n");

    // Ensure all slaves are in PRE-OP state
    ec_readstate();
    for(int i = 1; i <= ec_slavecount; i++) {
        if(ec_slave[i].state != EC_STATE_PRE_OP) {
            printf("Slave %d not in PRE-OP state. Current state: %d, StatusCode=0x%4.4x : %s\n", 
                   i, ec_slave[i].state, ec_slave[i].ALstatuscode, ec_ALstatuscode2string(ec_slave[i].ALstatuscode));
            return -1;
        }
    }

    // Configure distributed clock
    printf("Configuring DC...\n");
    ec_configdc();
    osal_usleep(200000);  // Wait for DC configuration to take effect

    // Request to switch to SAFE-OP state before confirming DC configuration
    for(int i = 1; i <= ec_slavecount; i++) {
        printf("Slave %d DC status: 0x%4.4x\n", i, ec_slave[i].DCactive);
        if(ec_slave[i].hasdc && !ec_slave[i].DCactive) {
            printf("DC not active for slave %d\n", i);
        }
    }

    // Request to switch to SAFE-OP state
    printf("Requesting SAFE_OP state...\n");
    ec_slave[0].state = EC_STATE_SAFE_OP;
    ec_writestate(0);
    osal_usleep(200000);  // Give enough time for state transition

    // Check the result of the state transition
    chk = 40;
    do {
        ec_readstate();
        for(int i = 1; i <= ec_slavecount; i++) {
            if(ec_slave[i].state != EC_STATE_SAFE_OP) {
                printf("Slave %d State=0x%2.2x StatusCode=0x%4.4x : %s\n",
                       i, ec_slave[i].state, ec_slave[i].ALstatuscode, ec_ALstatuscode2string(ec_slave[i].ALstatuscode));
            }
        }
        osal_usleep(100000);
    } while (chk-- && (ec_slave[0].state != EC_STATE_SAFE_OP));

    if (ec_slave[0].state != EC_STATE_SAFE_OP) {
        printf("Failed to reach SAFE_OP state\n");
        return -1;
    }

    printf("Successfully reached SAFE_OP state\n");

    // Calculate the expected Work Counter (WKC)
    expectedWKC = (ec_group[0].outputsWKC * 2) + ec_group[0].inputsWKC; // Calculate expected WKC based on outputs and inputs
    printf("Calculated workcounter %d\n", expectedWKC);

    // Read and display basic status information of the slaves
    ec_readstate(); // Read the state of all slaves
    for(int i = 1; i <= ec_slavecount; i++) {
        printf("Slave %d\n", i);
        printf("  State: %02x\n", ec_slave[i].state); // Print the state of the slave
        printf("  ALStatusCode: %04x\n", ec_slave[i].ALstatuscode); // Print the AL status code
        printf("  Delay: %d\n", ec_slave[i].pdelay); // Print the delay of the slave
        printf("  Has DC: %d\n", ec_slave[i].hasdc); // Check if the slave supports Distributed Clock
        printf("  DC Active: %d\n", ec_slave[i].DCactive); // Check if DC is active for the slave
        printf("  DC supported: %d\n", ec_slave[i].hasdc); // Print if DC is supported
    }

    // Read DC synchronization configuration using the correct parameters
    for(int i = 1; i <= ec_slavecount; i++) {
        uint16_t dcControl = 0; // Variable to hold DC control configuration
        int32_t cycleTime = 0; // Variable to hold cycle time
        int32_t shiftTime = 0; // Variable to hold shift time
        int size; // Variable to hold size for reading

        // Read DC synchronization configuration, adding the correct size parameter
        size = sizeof(dcControl);
        if (ec_SDOread(i, 0x1C32, 0x01, FALSE, &size, &dcControl, EC_TIMEOUTSAFE) > 0) {
            printf("Slave %d DC Configuration:\n", i);
            printf("  DC Control: 0x%04x\n", dcControl); // Print the DC control configuration
            
            size = sizeof(cycleTime);
            if (ec_SDOread(i, 0x1C32, 0x02, FALSE, &size, &cycleTime, EC_TIMEOUTSAFE) > 0) {
                printf("  Cycle Time: %d ns\n", cycleTime); // Print the cycle time
            }

        }
    }

    printf("__________STEP 6___________________\n");

    // Start the EtherCAT thread for real-time processing
    start_ecatthread_thread = TRUE; // Flag to indicate that the EtherCAT thread should start
    osal_thread_create_rt(&thread1, stack64k * 2, (void *)&ecatthread, (void *)&ctime_thread); // Create the real-time EtherCAT thread
    // set_thread_affinity(*thread1, 4); // Optional: Set CPU affinity for the thread
    osal_thread_create(&thread2, stack64k * 2, (void *)&ecatcheck, NULL); // Create the EtherCAT check thread
    // set_thread_affinity(*thread2, 5); // Optional: Set CPU affinity for the thread
    printf("___________________________________________\n");

    my_RA = 0; // Reset read access variable


    // 8. Transition to OP state
    printf("__________STEP 8___________________\n");

    // Send process data to the slaves
    ec_send_processdata();
    wkc = ec_receive_processdata(EC_TIMEOUTRET); // Receive process data and store the Work Counter

    // Set the first slave to operational state
    ec_slave[0].state = EC_STATE_OPERATIONAL; // Change the state of the first slave to OP
    ec_writestate(0); // Write the state change to the slave

    // Wait for the state transition to complete
    if ((ec_statecheck(0, EC_STATE_OPERATIONAL, 5 * EC_TIMEOUTSTATE)) == EC_STATE_OPERATIONAL) {
        printf("State changed to EC_STATE_OPERATIONAL: %d\n", EC_STATE_OPERATIONAL); // Confirm successful state change
        printf("___________________________________________\n");
    } else {
        printf("State could not be changed to EC_STATE_OPERATIONAL\n"); // Error message if state change fails
        for (int cnt = 1; cnt <= ec_slavecount; cnt++) {
            printf("ALstatuscode: %d\n", ecx_context.slavelist[cnt].ALstatuscode); // Print AL status codes for each slave
        }
    }

    // Read and display the state of all slaves
    ec_readstate(); // Read the state of all slaves
    for (int i = 1; i <= ec_slavecount; i++) {
        printf("Slave %d: Type %d, Address 0x%02x, State Machine actual %d, required %d\n", 
               i, ec_slave[i].eep_id, ec_slave[i].configadr, ec_slave[i].state, EC_STATE_OPERATIONAL); // Print slave information
        printf("Name: %s\n", ec_slave[i].name); // Print the name of the slave
        printf("___________________________________________\n");
    }

    // 9. Configure servomotor and mode operation
    printf("__________STEP 9___________________\n");

    if (ec_slave[0].state == EC_STATE_OPERATIONAL) {
        printf("Operational state reached for all slaves.\n");
        
        uint8 operation_mode = 9;  // CSV mode
        uint16_t Control_Word = 0;
        int32_t Max_Velocity = 5000;  // 最大速度限制
        int32_t Max_Acceleration = 5000;  // 最大加速度限制
        int32_t Quick_Stop_Decel = 10000;  // 快速停止减速度
        int32_t Profile_Decel = 5000;  // 减速度
        
        for (int i = 1; i <= ec_slavecount; i++) {
            // 先禁用电机
            Control_Word = 0x0000;
            ec_SDOwrite(i, 0x6040, 0x00, FALSE, sizeof(Control_Word), &Control_Word, EC_TIMEOUTSAFE);
            osal_usleep(100000);

            // 设置操作模式为CSV
            ec_SDOwrite(i, 0x6060, 0x00, FALSE, sizeof(operation_mode), &operation_mode, EC_TIMEOUTSAFE);
            osal_usleep(100000);

            // 设置速度相关参数
            ec_SDOwrite(i, 0x6080, 0x00, FALSE, sizeof(Max_Velocity), &Max_Velocity, EC_TIMEOUTSAFE);
            ec_SDOwrite(i, 0x60C5, 0x00, FALSE, sizeof(Max_Acceleration), &Max_Acceleration, EC_TIMEOUTSAFE);
            ec_SDOwrite(i, 0x6085, 0x00, FALSE, sizeof(Quick_Stop_Decel), &Quick_Stop_Decel, EC_TIMEOUTSAFE);
            ec_SDOwrite(i, 0x6084, 0x00, FALSE, sizeof(Profile_Decel), &Profile_Decel, EC_TIMEOUTSAFE);
            
            osal_usleep(100000);
            
            // 验证模式是否设置成功
            uint8 actual_mode;
            int size = sizeof(actual_mode);
            if (ec_SDOread(i, 0x6061, 0x00, FALSE, &size, &actual_mode, EC_TIMEOUTSAFE) > 0) {
                printf("Actual operation mode: %d\n", actual_mode);
            }
        }

        while(1) {
            osal_usleep(100000);
        }
    }

    osal_usleep(1e6);

    ec_close();

     printf("\nRequest init state for all slaves\n");
     ec_slave[0].state = EC_STATE_INIT;
     /* request INIT state for all slaves */
     ec_writestate(0);

    printf("EtherCAT master closed.\n");

    return 0;
}

/* 
 * PI calculation to synchronize Linux time with the Distributed Clock (DC) time.
 */
void ec_sync(int64 reftime, int64 cycletime, int64 *offsettime) {
    static int64 integral = 0;
    static int64 last_delta = 0;  // 用于计算微分项
    
    // 计算当前误差
    int64 delta = (reftime) % cycletime;
    if (delta > (cycletime / 2)) {
        delta = delta - cycletime;
    }
    
    // PI控制器参数
    const float Kp = 0.1;   // 比例系数
    const float Ki = 0.05;  // 积分系数
    
    // 计算积分项（带限幅）
    if (delta > 0) {
        integral = (integral < 10000) ? integral + 1 : integral;
    }
    if (delta < 0) {
        integral = (integral > -10000) ? integral - 1 : integral;
    }
    
    // 计算输出
    *offsettime = -(delta * Kp) - (integral * Ki);
    
    // 限制最大修正量
    if (*offsettime > cycletime/10) {
        *offsettime = cycletime/10;
    }
    if (*offsettime < -cycletime/10) {
        *offsettime = -cycletime/10;
    }
    
    last_delta = delta;
    gl_delta = delta;  // 用于调试
}

/* 
 * Add nanoseconds to a timespec structure.
 * This function updates the timespec structure by adding a specified amount of time.
 */
void add_timespec(struct timespec *ts, int64 addtime) {
    int64 sec, nsec; // Variables to hold seconds and nanoseconds

    nsec = addtime % NSEC_PER_SEC; // Calculate nanoseconds to add
    sec = (addtime - nsec) / NSEC_PER_SEC; // Calculate seconds to add
    ts->tv_sec += sec; // Update seconds in timespec
    ts->tv_nsec += nsec; // Update nanoseconds in timespec
    if (ts->tv_nsec >= NSEC_PER_SEC) { // If nanoseconds exceed 1 second
        nsec = ts->tv_nsec % NSEC_PER_SEC; // Adjust nanoseconds
        ts->tv_sec += (ts->tv_nsec - nsec) / NSEC_PER_SEC; // Increment seconds
        ts->tv_nsec = nsec; // Set adjusted nanoseconds
    }
}

// 添加电机状态定义
#define STATE_NOT_READY_TO_SWITCH_ON    0x0000
#define STATE_SWITCH_ON_DISABLED        0x0040
#define STATE_READY_TO_SWITCH_ON        0x0021
#define STATE_SWITCHED_ON               0x0023
#define STATE_OPERATION_ENABLED         0x0027
#define STATE_FAULT                     0x0008
#define STATE_FAULT_REACTION_ACTIVE     0x000F
#define STATE_QUICK_STOP_ACTIVE         0x0007

/* 
 * RT EtherCAT thread function
 */
OSAL_THREAD_FUNC_RT ecatthread(void *ptr) {
    struct timespec ts, tleft;
    int64 cycletime;
    struct timespec cycle_start, cycle_end;
    long cycle_time_ns;
    int64 dc_time_offset = 0;
    int dc_sync_counter = 0;
    const int DC_SYNC_INTERVAL = 10;  // 减小同步间隔，提高同步频率

    // 统计变量
    long min_cycle_time = LONG_MAX;
    long max_cycle_time = 0;
    long total_cycle_time = 0;
    int cycle_count = 0;
    int print_interval = 1000;

    // 初始化时间，与DC时钟同步
    clock_gettime(CLOCK_MONOTONIC, &ts);
    cycletime = *(int *)ptr * 1000;  // 转换为纳秒

    // 等待下一个整数周期开始
    ts.tv_nsec = (ts.tv_nsec / cycletime + 1) * cycletime;
    if (ts.tv_nsec >= NSEC_PER_SEC) {
        ts.tv_sec++;
        ts.tv_nsec -= NSEC_PER_SEC;
    }

    // 初始化DC同步
    toff = 0;
    if (ec_slave[0].hasdc) {
        // 获取当前DC时间
        dc_time_offset = ec_DCtime;
        // 计算与系统时间的偏差
        int64 system_time = ts.tv_sec * NSEC_PER_SEC + ts.tv_nsec;
        toff = dc_time_offset - system_time;
    }

    // 发送初始数据
    rxpdo.controlword = 0x0080;
    rxpdo.target_velocity = 0;
    rxpdo.mode_of_operation = 9;
    rxpdo.padding = 0;
    
    ec_send_processdata();
    wkc = ec_receive_processdata(EC_TIMEOUTRET);

    // 为每个从站维护状态记录
    uint16_t slave_last_status[EC_MAXSLAVE] = {0};

    while (1) {
        // 开始测量周期时间
        clock_gettime(CLOCK_MONOTONIC, &cycle_start);

        // 1. DC同步处理
        if (ec_slave[0].hasdc) {
            dc_sync_counter++;
            if (dc_sync_counter >= DC_SYNC_INTERVAL) {
                // 获取当前DC时间
                int64 dc_time = ec_DCtime;
                // 计算期望的下一个周期时间
                int64 next_dc = dc_time + cycletime;
                // 调整系统时间以匹配DC时钟
                ec_sync(dc_time, cycletime, &toff);
                dc_sync_counter = 0;
            }
        }

        // 2. 计算下一个周期的精确时间点
        int64 next_cycle = (ts.tv_sec * NSEC_PER_SEC + ts.tv_nsec) + cycletime + toff;
        ts.tv_sec = next_cycle / NSEC_PER_SEC;
        ts.tv_nsec = next_cycle % NSEC_PER_SEC;

        // 3. 发送过程数据
        ec_send_processdata();
        
        // 4. 精确等待下一个周期
        if (clock_nanosleep(CLOCK_MONOTONIC, TIMER_ABSTIME, &ts, NULL) != 0) {
            printf("clock_nanosleep error\n");
            continue;
        }

        // 5. 接收和处理数据
        wkc = ec_receive_processdata(EC_TIMEOUTRET);
        if (wkc >= expectedWKC) {
            for (int slave = 1; slave <= ec_slavecount; slave++) {
                memcpy(&txpdo, ec_slave[slave].inputs, sizeof(txpdo_t));
                uint16_t status = txpdo.statusword & 0x006F;
                
                // 只在状态真正发生变化时处理
                if (status != slave_last_status[slave]) {
                    printf("Slave %d: State changed from 0x%04x to 0x%04x\n", 
                           slave, slave_last_status[slave], status);
                    
                    // 根据状态变化发送相应命令
                    switch(status) {
                        case STATE_FAULT:
                            rxpdo.controlword = 0x0080;  // Fault reset
                            printf("Slave %d: Fault detected, sending reset\n", slave);
                            break;
                            
                        case STATE_SWITCH_ON_DISABLED:
                            rxpdo.controlword = 0x0006;  // Shutdown command
                            printf("Slave %d: Sending shutdown command\n", slave);
                            break;
                            
                        case STATE_READY_TO_SWITCH_ON:
                            rxpdo.controlword = 0x0007;  // Switch on command
                            printf("Slave %d: Sending switch on command\n", slave);
                            break;
                            
                        case STATE_SWITCHED_ON:
                            rxpdo.controlword = 0x000F;  // Enable operation command
                            printf("Slave %d: Sending enable operation command\n", slave);
                            break;
                            
                        case STATE_OPERATION_ENABLED:
                            rxpdo.controlword = 0x000F;  // Keep enabled
                            printf("Slave %d: Operation enabled\n", slave);
                            break;
                            
                        default:
                            rxpdo.controlword = 0x0006;  // Default to shutdown
                            printf("Slave %d: Unknown state 0x%04x\n", slave, status);
                            break;
                    }
                    
                    // 记录新状态
                    slave_last_status[slave] = status;
                } else {
                    // 保持当前状态的控制字
                    switch(status) {
                        case STATE_OPERATION_ENABLED:
                            rxpdo.controlword = 0x000F;  // Keep enabled
                            break;
                        default:
                            // 保持最后一次发送的控制字
                            break;
                    }
                }
                
                // 始终保持速度为0和CSV模式
                rxpdo.target_velocity = 0;
                rxpdo.mode_of_operation = 9;
                
                // 更新输出
                memcpy(ec_slave[slave].outputs, &rxpdo, sizeof(rxpdo_t));
            }
        }
        
        // 结束测量周期时间
        clock_gettime(CLOCK_MONOTONIC, &cycle_end);
        cycle_time_ns = (cycle_end.tv_sec - cycle_start.tv_sec) * NSEC_PER_SEC +
                       (cycle_end.tv_nsec - cycle_start.tv_nsec);
        
        // 更新统计信息
        min_cycle_time = std::min(min_cycle_time, cycle_time_ns);
        max_cycle_time = std::max(max_cycle_time, cycle_time_ns);
        total_cycle_time += cycle_time_ns;
        cycle_count++;
        
        // 定期打印统计信息
        if (cycle_count >= print_interval) {
            printf("\nPDO Cycle Statistics:\n");
            printf("Min cycle time: %ld ns\n", min_cycle_time);
            printf("Max cycle time: %ld ns\n", max_cycle_time);
            printf("Avg cycle time: %ld ns\n", total_cycle_time / cycle_count);
            printf("Expected cycle time: %ld ns\n", cycletime);
            printf("Overruns: %d\n\n", cycle_time_ns > cycletime * 1.2 ? 1 : 0);
            
            // 重置统计
            min_cycle_time = LONG_MAX;
            max_cycle_time = 0;
            total_cycle_time = 0;
            cycle_count = 0;
        }
        
        if (cycle_time_ns > cycletime * 1.2) {
            printf("WARNING: Cycle time exceeded: %ld ns (expected: %ld ns)\n", 
                   cycle_time_ns, cycletime);
        }
    }
}

/* 
 * EtherCAT check thread function
 */
OSAL_THREAD_FUNC ecatcheck(void *ptr) {
    // Get and print current CPU core
    int cpu = sched_getcpu();
    printf("EtherCAT check thread running on CPU %d\n", cpu);
    
    int slave;
    (void)ptr; // Not used
    int consecutive_errors = 0;
    const int MAX_CONSECUTIVE_ERRORS = 5;

    while (1) {
        if (inOP && ((wkc < expectedWKC) || ec_group[currentgroup].docheckstate)) {
            if (needlf) {
                needlf = FALSE;
                printf("\n");
            }
            
            // Increase the consecutive error count
            if (wkc < expectedWKC) {
                consecutive_errors++;
                printf("WARNING: Working counter error (%d/%d), consecutive errors: %d\n", 
                       wkc, expectedWKC, consecutive_errors);
            } else {
                consecutive_errors = 0;
            }

            // If the consecutive errors exceed the threshold, attempt reinitialization
            if (consecutive_errors >= MAX_CONSECUTIVE_ERRORS) {
                printf("ERROR: Too many consecutive errors, attempting recovery...\n");
                ec_group[currentgroup].docheckstate = TRUE;
                // Reset the error count
                consecutive_errors = 0;
            }

            ec_group[currentgroup].docheckstate = FALSE;
            ec_readstate();
            for (slave = 1; slave <= ec_slavecount; slave++) {
                if ((ec_slave[slave].group == currentgroup) && (ec_slave[slave].state != EC_STATE_OPERATIONAL)) {
                    ec_group[currentgroup].docheckstate = TRUE;
                    if (ec_slave[slave].state == (EC_STATE_SAFE_OP + EC_STATE_ERROR)) {
                        printf("ERROR: Slave %d is in SAFE_OP + ERROR, attempting ack.\n", slave);
                        ec_slave[slave].state = (EC_STATE_SAFE_OP + EC_STATE_ACK);
                        ec_writestate(slave);
                    } else if (ec_slave[slave].state == EC_STATE_SAFE_OP) {
                        printf("WARNING: Slave %d is in SAFE_OP, changing to OPERATIONAL.\n", slave);
                        ec_slave[slave].state = EC_STATE_OPERATIONAL;
                        ec_writestate(slave);
                    } else if (ec_slave[slave].state > EC_STATE_NONE) {
                        if (ec_reconfig_slave(slave, EC_TIMEOUTMON)) {
                            ec_slave[slave].islost = FALSE;
                            printf("MESSAGE: Slave %d reconfigured\n", slave);
                        }
                    } else if (!ec_slave[slave].islost) {
                        ec_statecheck(slave, EC_STATE_OPERATIONAL, EC_TIMEOUTRET);
                        if (!ec_slave[slave].state) {
                            ec_slave[slave].islost = TRUE;
                            printf("ERROR: Slave %d lost\n", slave);
                        }
                    }
                }
                if (ec_slave[slave].islost) {
                    if (!ec_slave[slave].state) {
                        if (ec_recover_slave(slave, EC_TIMEOUTMON)) {
                            ec_slave[slave].islost = FALSE;
                            printf("MESSAGE: Slave %d recovered\n", slave);
                        }
                    } else {
                        ec_slave[slave].islost = FALSE;
                        printf("MESSAGE: Slave %d found\n", slave);
                    }
                }
            }
            if (!ec_group[currentgroup].docheckstate) {
                printf("OK: All slaves resumed OPERATIONAL.\n");
            }
        }
        osal_usleep(10000); 
    }
}

int correct_count = 0;
int incorrect_count = 0;
int test_count_sum = 100;
int test_count = 0;
float correct_rate = 0;

// Main function
int main(int argc, char **argv) {
    // Add sched.h header if not already included
    #include <sched.h>
    
    needlf = FALSE;
    inOP = FALSE;
    
    // Get and print current CPU core for main thread
    int cpu = sched_getcpu();
    printf("Main thread running on CPU %d\n", cpu);
    
    start_ecatthread_thread = FALSE;
    dorun = 0;

    ctime_thread = 500;  // Communication period

    // Set the highest real-time priority
    struct sched_param param;
    param.sched_priority = 99;
    if (sched_setscheduler(0, SCHED_FIFO, &param) == -1) {
        perror("sched_setscheduler failed");
    }

    // Lock memory
    if (mlockall(MCL_CURRENT | MCL_FUTURE) == -1) {
        perror("mlockall failed");
    }

    // 修改 CPU affinity 设置
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(2, &cpuset);  // 只使用 CPU core 2

    if (sched_setaffinity(0, sizeof(cpu_set_t), &cpuset) == -1) {
        perror("sched_setaffinity");
        return EXIT_FAILURE;
    }

    printf("Running on CPU core 2\n");

    // 创建线程时也分别设置 affinity
    start_ecatthread_thread = FALSE;
    dorun = 0;
    ctime_thread = 1000;

    // 创建实时线程
    osal_thread_create_rt(&thread1, stack64k * 2, (void *)&ecatthread, (void *)&ctime_thread);
    
    // 获取实际的 pthread_t 并设置 affinity
    pthread_t* pthread1 = (pthread_t*)thread1;
    if (pthread1) {
        pthread_setaffinity_np(*pthread1, sizeof(cpu_set_t), &cpuset);
    }

    // 监控线程使用另一个核心
    cpu_set_t check_cpuset;
    CPU_ZERO(&check_cpuset);
    CPU_SET(3, &check_cpuset);  // 监控线程使用 CPU core 3
    
    // 创建监控线程
    osal_thread_create(&thread2, stack64k * 2, (void *)&ecatcheck, NULL);
    
    // 获取实际的 pthread_t 并设置 affinity
    pthread_t* pthread2 = (pthread_t*)thread2;
    if (pthread2) {
        pthread_setaffinity_np(*pthread2, sizeof(cpu_set_t), &check_cpuset);
    }

    printf("EtherCAT RT thread bound to CPU 2\n");
    printf("EtherCAT check thread bound to CPU 3\n");

    erob_test();
    printf("End program\n");

    return EXIT_SUCCESS;
}
