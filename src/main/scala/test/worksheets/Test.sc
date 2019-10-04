val string = "23:10"
val flag =
  (string.charAt(0) - '0' == 0 && string.charAt(1) - '0' == 9) ||
    ((string.charAt(0) - '0' == 1 && string.charAt(1) - '0' <= 3) &&
      (string.charAt(0) - '0' == 1 && string.charAt(1) - '0' >= 5) ||
      (string.charAt(0) - '0' == 1 && string.charAt(1) - '0' <= 7))