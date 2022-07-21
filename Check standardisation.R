library(dplyr)

d2020 <- read.table(
  textConnection(
    "Age	Unhealthy	Duration	Value
0	0	0	10
0	0	1	11
0	0	2	12
0	0	3	13
0	1	0	20
0	1	1	21
0	1	2	22
0	1	3	23
1	0	0	30
1	0	1	31
1	0	2	32
1	0	3	33
1	1	0	40
1	1	1	41
1	1	2	42
1	1	3	43
2	0	0	50
2	0	1	51
2	0	2	52
2	0	3	53
2	1	0	60
2	1	1	61
2	1	2	62
2	1	3	63
"
  ),
  header = TRUE,
  sep = "\t"
)

d2021 <- read.table(
  textConnection(
    "Age	Unhealthy	Duration	Value
0	0	0	13
0	0	1	22
0	0	2	38
0	0	3	72
0	1	0	15
0	1	1	27
0	1	2	47
0	1	3	84
1	0	0	38
1	0	1	56
1	0	2	84
1	0	3	132
1	1	0	32
1	1	1	51
1	1	2	80
1	1	3	126
2	0	0	60
2	0	1	82
2	0	2	113
2	0	3	159
2	1	0	51
2	1	1	74
2	1	2	105
2	1	3	150"
  ),
  header = TRUE,
  sep = "\t"
)

l2020 <- read.table(
  textConnection(
    "Age	Unhealthy	Duration	Value
0	0	0	10000
0	0	1	8000
0	0	2	6000
0	0	3	4000
0	1	0	11000
0	1	1	9000
0	1	2	7000
0	1	3	5000
1	0	0	12000
1	0	1	10000
1	0	2	8000
1	0	3	6000
1	1	0	13000
1	1	1	11000
1	1	2	9000
1	1	3	7000
2	0	0	15000
2	0	1	13000
2	0	2	11000
2	0	3	9000
2	1	0	16000
2	1	1	14000
2	1	2	12000
2	1	3	10000"
  ),
  header = TRUE,
  sep = "\t"
)

l2021 <- read.table(
  textConnection(
    "Age	Unhealthy	Duration	Value
0	0	0	13000
0	0	1	16000
0	0	2	19000
0	0	3	22000
0	1	0	8000
0	1	1	11000
0	1	2	14000
0	1	3	17000
1	0	0	15000
1	0	1	18000
1	0	2	21000
1	0	3	24000
1	1	0	10000
1	1	1	13000
1	1	2	16000
1	1	3	19000
2	0	0	18000
2	0	1	21000
2	0	2	24000
2	0	3	27000
2	1	0	13000
2	1	1	16000
2	1	2	19000
2	1	3	22000
"
  ),
  header = TRUE,
  sep = "\t"
)

# First we expand the data out into multiple rows to match what's on Comodash

l2020f <- l2020[rep(1:24, l2020$Value), 1:3]
l2020f$expos <- 1
l2020f$deaths <- 0

# Check
nrow(l2020f[l2020f$Unhealthy == 1 &
              l2020f$Age == 0, ]) # 32000 is correct

l2021f <- l2021[rep(1:24, l2021$Value), 1:3]
l2021f$expos <- 1
l2021f$deaths <- 0

set.seed(22)

# We'll take the number of deaths for each cell from the workbook and randomly allocate them to an exposure line

d2020f <- apply(d2020, 1, FUN = \(x) {
  deaths = x[4]
  rows <-
    (1:nrow(l2020f))[l2020f$Age == x[1] &
                       l2020f$Unhealthy == x[2] & l2020f$Duration == x[3]]
  
  dthrows <- sample(rows, size = deaths)
  
  dthrows
})

l2020f[unlist(d2020f), "deaths"] <- 1

d2021f <- apply(d2021, 1, FUN = \(x) {
  deaths = x[4]
  rows <-
    (1:nrow(l2021f))[l2021f$Age == x[1] &
                       l2021f$Unhealthy == x[2] & l2021f$Duration == x[3]]
  
  dthrows <- sample(rows, size = deaths)
  
  dthrows
})

l2021f[unlist(d2021f), "deaths"] <- 1

# Check
sum(l2021f$deaths) # 1711 - off due to rounding

sum(l2021f[l2021f$Age == 0 &
             l2021f$Unhealthy == 1, "deaths"]) # 174 - off by one due to rounding

# Now we calculate standardisation factors, and attach them to the dataframes

durfactors <-
  unlist((l2021 %>% group_by(Duration) %>% summarise(sum(Value) / sum(l2021$Value)))[, 2])
agefactors <-
  unlist((l2021 %>% group_by(Age) %>% summarise(sum(Value) / sum(l2021$Value)))[, 2])
healthfactors <-
  unlist((l2021 %>% group_by(Unhealthy) %>% summarise(sum(Value) / sum(l2021$Value)))[, 2])

for (i in 1:4) {
  l2020f[l2020f$Duration == i - 1, "durfactor"] <- durfactors[i]
  l2021f[l2021f$Duration == i - 1, "durfactor"] <- durfactors[i]
}

for (i in 1:3) {
  l2020f[l2020f$Age == i - 1, "agefactor"] <- agefactors[i]
  l2021f[l2021f$Age == i - 1, "agefactor"] <- agefactors[i]
}

for (i in 1:2) {
  l2020f[l2020f$Unhealthy == i - 1, "healthfactor"] <- healthfactors[i]
  l2021f[l2021f$Unhealthy == i - 1, "healthfactor"] <-
    healthfactors[i]
}

lall <- rbind(cbind(Year=2020, l2020f), cbind(Year=2021, l2021f))

### Excel approach ------------------------------------------------

std_xlpt1 <-
  lall %>% group_by(Year, Age, Duration, Unhealthy, agefactor, healthfactor, durfactor) %>%
  summarise(deaths = sum(deaths), expos = sum(expos)) %>%
  mutate(stdpop = agefactor * healthfactor * durfactor * 2000000, # using 2000000 to allow for having two years
         cruderate = deaths / expos,
         stddeaths = cruderate * stdpop) # we use pt1 to check our crude rates and standard pop match

std_xlpt2_age <- std_xlpt1 %>% group_by(Year, Age) %>% summarise(sum(stddeaths)/sum(stdpop)) # Matches Workbook

std_xlpt2_healthy <- std_xlpt1 %>% group_by(Year, Unhealthy) %>% summarise(sum(stddeaths)/sum(stdpop)) # Also matches workbook

### Comodash approach ----------------------------------------------

lall %>% group_by(Year, Age, Duration, Unhealthy) %>%
  mutate(std_factor = agefactor * durfactor * healthfactor, expos_factor = 1 / sum(expos), simplefac=1/n()) %>%
  group_by(Year, Age) %>% summarise(sum(deaths * std_factor * expos_factor)/sum(std_factor * simplefac)) # Matches above


lall %>% group_by(Year, Age, Duration, Unhealthy) %>%
  mutate(std_factor = agefactor * durfactor * healthfactor, expos_factor = 1 / sum(expos), simplefac=1/n()) %>%
  group_by(Year, Unhealthy) %>% summarise(sum(deaths * std_factor * expos_factor)/sum(std_factor * simplefac)) # Matches above

### Let's check fractional exposure results ------------------------

# We'll randomly split some rows into two parts.

lall2 <- lall

set.seed(24)

randomrows <- sample((1:nrow(lall2))[lall2$deaths==0], size=nrow(lall2)/2)

randomrows2 <- sample((1:nrow(lall2))[lall2$deaths==1], size=sum(lall2$deaths)/2)

lall2[randomrows, "expos"] <- 0.5
lall2 <- rbind(lall2, lall2[randomrows,])

lall2[randomrows2, "expos"] <- 0.5
lall2 <- rbind(lall2, lall2[randomrows2,])
lall2[randomrows2, "deaths"] <- 0 # We don't want to have duplicated the numbers of deaths for these rows

sum(lall2$expos)
sum(lall2$deaths) # both good

lall2 %>% group_by(Year, Age, Duration, Unhealthy) %>%
  mutate(std_factor = agefactor * durfactor * healthfactor, expos_factor = 1 / sum(expos), simplefac=1/n()) %>%
  group_by(Year, Age) %>% summarise(sum(deaths * std_factor * expos_factor)/sum(std_factor * simplefac)) # Matches above

lall2 %>% group_by(Year, Age, Duration, Unhealthy) %>%
  mutate(std_factor = agefactor * durfactor * healthfactor, expos_factor = 1 / sum(expos), simplefac=1/n()) %>%
  group_by(Year, Unhealthy) %>% summarise(sum(deaths * std_factor * expos_factor)/sum(std_factor * simplefac)) # Matches above
