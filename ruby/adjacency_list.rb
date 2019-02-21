adjacency_list = {
  0  => [1],
  1  => [0, 2],
  2  => [1, 3],
  3  => [2, 4, 5],
  4  => [3],
  5  => [3, 6],
  6  => [5, 7, 9],
  9  => [6, 10],
  10 => [9, 11],
  11 => [10, 12],
  12 => [11, 13],
  13 => [12]
}

# curr_node = adjacency_list.keys.sort[0]
# end_node = adjacency_list.keys.sort[-1]
curr_node = 0
end_node = 13
step = 1
while true do
  next_node = adjacency_list[curr_node].max
  puts "step #{step}: current node is #{curr_node} and next node is #{next_node}"
  step += 1
  curr_node = adjacency_list[next_node].max
  if curr_node == end_node
    puts "step #{step}: current node is #{curr_node} and next node is n/a"
    break
  end
end
