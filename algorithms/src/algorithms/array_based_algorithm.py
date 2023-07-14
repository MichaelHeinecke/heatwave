heatwaves = []
dates = [
    "2004-06-01",
    "2004-06-02",
    "2004-06-03",
    "2004-06-04",
    "2004-06-05",
    "2004-06-06",
    "2004-06-07",
    "2004-06-08",
    "2004-06-09",
    "2004-06-10",
    "2004-06-11",
    "2004-06-12",
]
temperatures = [21, 24, 22, 25, 26, 30, 30, 25, 31, 25, 22, 25]

start_index = None
end_index = None
number_of_days = 0
number_of_tropical_days = 0
max_temp = 0

for i in range(0, len(temperatures)):
    if temperatures[i] >= 25:
        number_of_days += 1
        end_index = i
        if start_index is None:
            start_index = i

    if temperatures[i] >= 30:
        number_of_tropical_days += 1

    if temperatures[i] > max_temp:
        max_temp = temperatures[i]

    # If sequence of days with maximum temperatures of at least 25 is interrupted,
    # or we reach the end of the input array.
    if temperatures[i] < 25 or i == len(temperatures) - 1:
        # Check if we found a heatwave
        if number_of_days >= 5 and number_of_tropical_days >= 3:
            heatwaves.append(
                (
                    dates[start_index],
                    dates[end_index],
                    number_of_days,
                    number_of_tropical_days,
                    max_temp,
                )
            )

        # Reset variables to check next potential heatwave.
        start_index = None
        end_index = None
        number_of_days = 0
        number_of_tropical_days = 0
        max_temp = 0

for start_date, end_date, duration, number_tropical_days, max_temp in heatwaves:
    print(
        f"From date: {start_date}, to date: {end_date}, "
        f"duration: {duration}, number of tropical days: {number_tropical_days}, "
        f"maximum temperature: {max_temp}"
    )
